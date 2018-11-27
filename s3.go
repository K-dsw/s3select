package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var wg sync.WaitGroup
var count int64
var filecount int

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
				fmt.Printf("Processed %d bytes\n", *e.Details.BytesProcessed)
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
		fmt.Println(record)
	}
	if err := resp.EventStream.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "reading from event stream failed, %v\n", err)
	}

	wg.Done()
}

func main() {

	// Variables
	bucket := "seselect"

	// Create a session and error object.
	sess, err := session.NewSession(&aws.Config{Region: aws.String(os.Getenv("REGION"))})
	if err != nil {
		panic(err)
	}

	// Create the s3 session
	svc := s3.New(sess)

	// Create our objects
	objects, err := svc.ListObjects(&s3.ListObjectsInput{Bucket: &bucket})

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
}
