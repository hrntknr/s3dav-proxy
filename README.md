# s3dav-proxy

## Abount s3dav-proxy
s3dav-proxy is software that allows s3 compatible file servers to be operated via the webdav protocol.  
It uses webdav authentication information as it is as an s3 access key/secret, so it is unique in that it can reuse s3 access privileges, etc.  
It is intended to be self-hosted with minio, but will also work with s3.

## Getting Started
- Create docker-compose.yml file with the following code.
```
version: '3'
services:
  minio:
    image: minio/minio
    ports:
      - 9000:9000
      - 9001:9001
    environment:
      MINIO_ACCESS_KEY: minio
      MINIO_SECRET_KEY: minio123
    command: server /data --console-address ":9001"
  s3dav:
    image: hrntknr/s3dav-proxy
    ports:
      - 8080:8080
    command: --endpoint minio:9000 --allow-buckets-ops
```
- Run `docker-compose up -d`.

## Supported Clients
- linux nautilus
- windows explorer
  - windows restricts files over 50 MB. ([Link](https://support.microsoft.com/topic/815e2949-0f56-ec25-db7d-b6d860a31f77))
  - Basic authentication without TLS is not allowed in windows. ([Link](https://learn.microsoft.com/ja-jp/archive/blogs/robert_mcmurray/webdav-redirector-registry-settings))

## Performance
write/upload is slower, but read/download is available at higher speeds.  
Improvements to upload speed will be made in the future.

```
$ dd if=/dev/zero of=./dummy bs=1G count=10
$ time -p curl -T ./dummy http://minio:minio123@localhost:8080/test/test -o /dev/null -s
real 62.68
user 0.09
sys 2.64
$ time -p curl http://minio:minio123@localhost:8080/test/test -o /dev/null -s
real 18.67
user 0.68
sys 2.36
$ time -p mc cp minio/test/dummy ./
real 26.70
user 2.72
sys 13.61
$ time -p mc cp minio/test/dummy ./
sreal 19.05
user 2.64
sys 13.12
```
