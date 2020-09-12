package main

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxPartSize        = int64(5 * 1024 * 1024)
	maxRetries         = 3
	awsAccessKeyID     = "Your access key"
	awsSecretAccessKey = "Your secret key"
	awsBucketRegion    = "S3 bucket region"
	awsBucketName      = "S3 bucket name"
	maxConnection      = 5
)

//UploadTask is a worker task for channel
type UploadTask struct {
	FileBytes  []byte
	PartNumber int
	TotalParts int
	Bucket     string
	Key        string
	UploadID   string
}

func main() {
	targetPath := flag.String("path", "", "-path /path with front slash, default is root folder")
	/*
		Storage class Possible values:
		https://docs.aws.amazon.com/cli/latest/reference/s3api/create-multipart-upload.html

		STANDARD
		REDUCED_REDUNDANCY
		STANDARD_IA
		ONEZONE_IA
		INTELLIGENT_TIERING
		GLACIER
		DEEP_ARCHIVE
	*/
	mystorageClass := flag.String("storage", "STANDARD", "-storage STANDARD/GLACIER/DEEP_ARCHIVE, default is STANDARD")

	filename := flag.String("file", "filename", "-file filename")
	flag.Parse()

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: aws-multipart-upload -file [filename] optional flags:[-path][-storage]\n")

		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(os.Stderr, "    %v\n", f.Usage) // f.Name, f.Value
		})
	}

	if len(os.Args) == 1 {
		flag.Usage()
		return
	}

	fmt.Printf("upload file:%s to %s\n", *filename, *mystorageClass)

	file, err := os.Open(*filename)
	if err != nil {
		fmt.Printf("err opening file: %s", err)
		return
	}
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)
	fileType := http.DetectContentType(buffer)
	file.Read(buffer)

	path := *targetPath + "/" + file.Name()
	input := &s3.CreateMultipartUploadInput{
		Bucket:       aws.String(awsBucketName),
		Key:          aws.String(path),
		ContentType:  aws.String(fileType),
		StorageClass: aws.String(*mystorageClass),
	}

	var totalparts = int(math.Ceil(float64(size) / float64(maxPartSize)))

	fmt.Printf("Total parts: %v  Size: %v\n", totalparts, size)

	creds := credentials.NewStaticCredentials(awsAccessKeyID, awsSecretAccessKey, "")
	_, err2 := creds.Get()
	if err2 != nil {
		fmt.Printf("bad credentials: %s", err2)
	}
	cfg := aws.NewConfig().WithRegion(awsBucketRegion).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)

	resp, err := svc.CreateMultipartUpload(input)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("Created multipart upload request upload id = %s\n", *resp.UploadId)

	var curr, partLength int64
	var remaining = size
	var completedParts []*s3.CompletedPart
	partNumber := 1

	//receive completed parts from upload result
	partsChannel := make(chan *s3.CompletedPart, int(totalparts))
	//send out UploadTask to worker
	tasksChannel := make(chan *UploadTask, int(totalparts))

	for myworker := 1; myworker <= maxConnection; myworker++ {
		go uploadPart(myworker, tasksChannel, partsChannel, svc)
	}

	for curr = 0; remaining != 0; curr += partLength {

		if remaining < maxPartSize {
			partLength = remaining
		} else {
			partLength = maxPartSize
		}

		myTask := new(UploadTask)
		myTask.FileBytes = buffer[curr : curr+partLength]
		myTask.PartNumber = partNumber
		myTask.Bucket = *resp.Bucket
		myTask.Key = *resp.Key
		myTask.UploadID = *resp.UploadId
		myTask.TotalParts = totalparts

		tasksChannel <- myTask

		remaining -= partLength

		partNumber++

	}
	//all tasks were sent and queueing
	close(tasksChannel)

	//we need partsChannel to block here
	for parts := range partsChannel {
		completedParts = append(completedParts, parts)
		if len(completedParts) == totalparts {
			close(partsChannel)
			break
		}
	}
	fmt.Println("sorting result parts...")
	//sort completedParts by partnumber as we randomly added in goroutine, otherwise s3 would complain
	sort.Slice(completedParts, func(i, j int) bool {
		return *(completedParts[i].PartNumber) < *(completedParts[j].PartNumber)
	})

	completeResponse, err := completeMultipartUpload(svc, resp, completedParts)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	fmt.Printf("Successfully uploaded file: %s\n", completeResponse.String())
}

func completeMultipartUpload(svc *s3.S3, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {

	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return svc.CompleteMultipartUpload(completeInput)
}

func uploadPart(id int, tasks <-chan *UploadTask, resultChannel chan<- *s3.CompletedPart, svc *s3.S3) {

	for task := range tasks {
		fmt.Printf("Worker %d start Upload part: %v\n", id, task.PartNumber)

		tryNum := 1
		partInput := &s3.UploadPartInput{
			Body:          bytes.NewReader(task.FileBytes),
			Bucket:        &task.Bucket,
			Key:           &task.Key,
			PartNumber:    aws.Int64(int64(task.PartNumber)),
			UploadId:      &task.UploadID,
			ContentLength: aws.Int64(int64(len(task.FileBytes))),
		}

		for tryNum <= maxRetries {
			uploadResult, err := svc.UploadPart(partInput)
			if err != nil {
				if tryNum == maxRetries {
					if aerr, ok := err.(awserr.Error); ok {
						abortMultipartUpload(svc, task, aerr)
					} else {
						abortMultipartUpload(svc, task, err)
					}
				}
				fmt.Printf("Worker %d Retry part: %v / %v\n", id, task.PartNumber, task.TotalParts)
				tryNum++
			} else {
				fmt.Printf("Worker %d Finished part: %v / %v\n", id, task.PartNumber, task.TotalParts)
				resultChannel <- &s3.CompletedPart{
					ETag:       uploadResult.ETag,
					PartNumber: aws.Int64(int64(task.PartNumber)),
				}
				break
			}
		}
	}

}

func abortMultipartUpload(svc *s3.S3, task *UploadTask, err error) {
	fmt.Println("Aborting multipart upload for UploadId:" + task.UploadID)
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   &task.Bucket,
		Key:      &task.Key,
		UploadId: &task.UploadID,
	}
	_, abouterr := svc.AbortMultipartUpload(abortInput)
	if abouterr != nil {
		fmt.Println("AbortMultipartUpload also failed")
		fmt.Println(abouterr.Error())
	} else {
		fmt.Println("AbortMultipartUpload success")
	}
	panic(err)
}
