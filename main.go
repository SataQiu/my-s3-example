package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	ak     = "MYEXAMPLEACCESSKEY" //文件服务分配的账号
	sk     = "MYEXAMPLESECRETKEY" //文件服务分配的秘钥
	region = "eu-west-1"          //适用范围
	svc    *s3.S3

	endPoint = "http://127.0.0.1:8333" // seaweedfs S3 服务的地址
	objectID = "a.jpg"

	//endPoint = "http://127.0.0.1:8099" // S3 Proxy 服务的地址
	//objectID = "b.jpg"
)

func init() {
	cres := credentials.NewStaticCredentials(ak, sk, "")
	cfg := aws.NewConfig().WithRegion(region).WithEndpoint(endPoint).WithCredentials(cres).WithS3ForcePathStyle(true)
	sess, err := session.NewSession(cfg)
	if err != nil {
		fmt.Println(err)
	}
	svc = s3.New(sess)
}

func main() {
	// 创建桶
	bucketName := "files" //桶的名称也是存取这个桶下面数据的唯一标识
	createBucket(bucketName)
	// 将图片数据上传到weed文件服务
	dataImage, err := ioutil.ReadFile(objectID)
	if err != nil {
		fmt.Println(err.Error())
	}
	contentType := "image/jpeg"
	putS3Object(dataImage, bucketName, contentType, objectID)
	// 获取图片
	res := getS3Object(bucketName, objectID)
	fmt.Println(base64.StdEncoding.EncodeToString(res))
	// 按照对象key删除对象信息
	// deleteS3Object(bucketName, objectID)
}

func createBucket(bucketName string) {
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	result, err := svc.CreateBucket(input)
	fmt.Println(result)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				fmt.Println(s3.ErrCodeBucketAlreadyExists, aerr.Error())
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				fmt.Println(s3.ErrCodeBucketAlreadyOwnedByYou, aerr.Error())
			default:
				fmt.Println(aerr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
	}
}

func putS3Object(dataImage []byte, bucketName, contentType, objectID string) {

	inputObject := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectID),
		ContentType: aws.String(contentType),
		Body:        bytes.NewReader(dataImage),
	}
	resp, err := svc.PutObject(inputObject)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
}

func getS3Object(bucketName, objectID string) []byte {

	inputObject := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectID),
	}
	out, err := svc.GetObject(inputObject)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	res, err := ioutil.ReadAll(out.Body)
	if err != nil {
		fmt.Println(err.Error())
		return nil
	}
	return res
}

func deleteS3Object(bucketName, objectID string) {
	params := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectID),
	}

	resp, err := svc.DeleteObject(params)
	if err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println(resp)
}
