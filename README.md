## Running

### Scrape without limits

`scrapy crawl <spider-name>`

### Limit number of results to <#>

`scrapy crawl <spider-name> -s  CLOSESPIDER_ITEMCOUNT=<#>`

_Concurrent requests not in queue still executed_

### Limit articles no older than YYYYMMDDHHMMSS

`scrapy crawl <spider-name> -a dt-from=20231012153000`

### Don't save save results in files (useful for testing)

`scrapy crawl <spider-name> -a save=false`
