package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Variables
var wg sync.WaitGroup
var count int64
var filecount int

var threshold = map[string]int{
	"overThreshold":  0,
	"underThreshold": 0,
}

// IsNumeric checks and make sure it's numeric string
func IsNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func s3select(s3object *s3.SelectObjectContentInput, svc *s3.S3) {
	resp, err := svc.SelectObjectContent(s3object)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed making API request, %v\n", err)
		return
	}

	defer resp.EventStream.Close()

	results, resultWriter := io.Pipe()
	go func() {
		defer resultWriter.Close()
		for event := range resp.EventStream.Events() {
			switch e := event.(type) {
			case *s3.RecordsEvent:
				resultWriter.Write(e.Payload)
			case *s3.StatsEvent:
				megabytes := *e.Details.BytesProcessed / 1024 / 1024
				fmt.Printf("Processed %d megabytes\n", megabytes)
				count = count + *e.Details.BytesProcessed
			}
		}
	}()

	// Printout the results
	resReader := csv.NewReader(results)
	for {
		record, err := resReader.Read()
		if err == io.EOF {
			break
		}
		str := strings.Join(record, " ")

		if IsNumeric(str) == false {
			continue
		}

		value, _ := strconv.ParseFloat(str, 64)

		if value > 100 {
			fmt.Println("Analytics Tripped: Value greater than 100 =>", value)
			threshold["overThreshold"] = threshold["overThreshold"] + 1
		} else {
			threshold["underThreshold"] = threshold["underThreshold"] + 1
		}
	}

	if err := resp.EventStream.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "reading from event stream failed, %v\n", err)
	}

	wg.Done()
}

func main() {

	// Variables
	bucket := "seselect-test"

	// Create a session and error object.
	sess, err := session.NewSession(&aws.Config{Region: aws.String(os.Getenv("REGION"))})
	if err != nil {
		panic(err)
	}

	// Create the s3 session
	svc := s3.New(sess)

	// Create our objects
	objects, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: &bucket})

	if err != nil {
		panic(err)
	}

	// Loop over our objects
	for _, b := range objects.Contents {
		fmt.Println(*b.Key)
		s3objs := s3.SelectObjectContentInput{
			Bucket:         aws.String(bucket),
			Key:            b.Key,
			Expression:     aws.String("SELECT s._4 from S3Object AS s WHERE s._1 = 'NET'"),
			ExpressionType: aws.String(s3.ExpressionTypeSql),
			InputSerialization: &s3.InputSerialization{
				CSV: &s3.CSVInput{
					FileHeaderInfo:             aws.String("None"),
					AllowQuotedRecordDelimiter: aws.Bool(true),
					QuoteCharacter:             aws.String("'"),
				},
				CompressionType: aws.String("GZIP"),
			},
			OutputSerialization: &s3.OutputSerialization{
				CSV: &s3.CSVOutput{},
			},
		}

		wg.Add(1)
		filecount = filecount + 1
		go s3select(&s3objs, svc)

	}

	wg.Wait()
	megabytes := count / 1024 / 1024
	fmt.Printf("Total Megabytes Processed: %d\n", megabytes)
	fmt.Printf("Total NMON Files Processed: %d\n", filecount)
	fmt.Printf("Total Number of Analytics Over Threshold: %d\n", threshold["overThreshold"])
	fmt.Printf("Total Number of Analytics Under Threshold: %d\n", threshold["underThreshold"])
}
