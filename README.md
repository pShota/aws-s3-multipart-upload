# AWS S3 Multipart Upload

This is a forked repo from [here](https://github.com/apoorvam/aws-s3-multipart-upload)

This repo added "Worker pool" by using **Goroutine** so that multiparts are uploaded in parallel

Also added a few flags to control `S3 Storage Class` and `Directory path`

### Variables setup before run:

      const (
          maxPartSize        = int64(5 * 1024 * 1024)
          maxRetries         = 3
          awsAccessKeyID     = "Your access key"
          awsSecretAccessKey = "Your secret key"
          awsBucketRegion    = "S3 bucket region"
          awsBucketName      = "S3 bucket name"
          maxConnection = 5
      )

### Usage:

      Usage: aws-multipart-upload -file [filename] optional flags:[-path][-storage]
      -file filename
      -path /path with front slash, default is root folder
      -storage STANDARD/GLACIER/DEEP_ARCHIVE, default is STANDARD


## Build prerequisite:
Go Lang: https://golang.org/doc/install

install aws go sdk: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/welcome.html
