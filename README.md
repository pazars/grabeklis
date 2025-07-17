## Running

Output data is stored in MongoDB.

Setup locally using Docker:

```
docker pull mongodb/mongodb-community-server:latest
```

```
docker run --name mongodb -p 27017:27017 -d mongodb/mongodb-community-server:latest
```

### Scrape without limits

`scrapy crawl <spider-name>`

### Limit number of results to <#>

`scrapy crawl <spider-name> -s  CLOSESPIDER_ITEMCOUNT=<#>`

_Concurrent requests not in queue still executed_

### Limit articles no older than YYYYMMDDHHMMSS

`scrapy crawl <spider-name> -a dt-from=20231012153000`

### Don't save save results in files (useful for testing)

`scrapy crawl <spider-name> -a save=false`


## Push image to GCP

First setup repository authentication.
Setup instructions are in GCP's Artifact Registry.

Then, build and push:

```
docker buildx build --platform linux/amd64 . -t [LOCAL_IMAGE_NAME]:[TAG]
docker tag [LOCAL_IMAGE_NAME]:[TAG] [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY_NAME]/[IMAGE_NAME]:[TAG]
docker push [REGION]-docker.pkg.dev/[PROJECT_ID]/[REPOSITORY_NAME]/[IMAGE_NAME]:[TAG]
```