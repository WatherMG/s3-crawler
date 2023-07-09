# s3-crawler

###RUN:

Change config file in root.
```json
{
  "s3Connection": {
    "endpoint": "https://localstack.localhost.com",
    "region": "eu-central-1",
    "accessKeyId": "xxxx",
    "secretAccessKey": "yyyy"
  },
  "bucketName": "bucketName",
  "s3prefix": "someFolder",
  "extension": "mp3", 
  "nameMask": "cuttedName",
  "minFileSizeMB": 0,
  "pagination": {
    "maxKeys": 1000
  },
  "NumCPU": 16,
  "compression": false,
  "withParts": false ,
  "downloadPath": "/tmp/upload/data"
}
```
Where extension, nameMask, minFileSizeMB - is filters for donwloading files.

To download from yandex s3 you don't need use hash with parts (set withParts=false).


```shell
cd /cmd/
go run crawler.go
```
