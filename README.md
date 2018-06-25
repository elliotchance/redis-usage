A non-blocking way to count the number of keys or size of Redis key prefixes.

```
Usage of ./redis-usage:
  -count int
    	SCAN COUNT option (default 10)
  -db int
    	redis server database
  -host string
    	redis server host (default "localhost")
  -limit int
    	limit the number of keys scanned
  -match string
    	SCAN MATCH option
  -port int
    	redis server port number (default 6379)
  -prefixes string
    	known prefixes to group
  -sleep int
    	number of milliseconds to wait between reading keys
  -timeout int
    	milliseconds for timeout (default 3000)
  -top int
    	only show the top number of prefixes
  -use-dump
    	use DUMP to get key sizes (much slower)
```
