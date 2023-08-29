# file to s3 demo

it is a demo to how to use spring integration to store file in S3 with db auditing reading files from a folder

## how to setup

- start the docker compose in order to have S3 in local
- execute the following command
```shell
aws s3api create-bucket --bucket file-to-s3-demo --endpoint http://localhost:4566 --region us-east-1
```

- start the application
- put files in the `loading-folder` folder
- enjoy ;)

## how to see if ti works
- use the endpoint http://localhost:8080/file-statistics to check the db entry
- use the following command to see in s3
```shell
aws s3 ls s3://file-to-s3-demo --endpoint http://localhost:4566 --region us-east-1
```