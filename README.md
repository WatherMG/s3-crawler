# s3-crawler

### RUN:

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
  "maxFileSizeMB": 0,
  "pagination": {
    "maxKeys": 1000,
    "maxPages": 0,
    "chunkSizeMB": 0
  },
  "numCPU": 4,
  "downloaders": 192,
  "isFlattenName": false,
  "decompress": false,
  "decompressWithDirName": false,
  "withParts": false,
  "downloadPath": "/mnt/c/data",
  "progress": {
    "withProgressBar": true,
    "delay": 500,
    "barSize": 20
  }
}
```
Where extension, nameMask, minFileSizeMB - is filters for downloading files.
isFlattenName - sets the file name by adding directory names with '_', removing directories from the path.
decompress - allows you to unpack archives (gzip) on the fly. Changes the file name by appending the suffix "_unpacked" to it.
decompressWithDirName - for each unpacked file creates a folder with the original file name, into which it saves the file.
If numCPU, downloaders, chunkSizeMB, maxPages is empty - will be used optimized values.

To download from yandex s3 you don't need use hash with parts (set withParts=false).


```shell
cd /cmd/
go run crawler.go -config=PATH_TO_CONFIG_FILE -profiling=true (defalut false)
```
