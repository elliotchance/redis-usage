A non-blocking way to count the number of keys or size of Redis key prefixes.

```
Usage of ./redis-usage:
  -count int
    	SCAN COUNT option. (default 10)
  -db int
    	Redis server database.
  -dump-limit int
    	Use DUMP to get key sizes (much slower). If this is zero then DUMP will not be used, otherwise it will take N sizes for each prefix to calculate an average bytes for that key prefix. If you want to measure the sizes for all keys set this to a very large number.
  -host string
    	Redis server host. (default "localhost")
  -limit int
    	Limit the number of keys scanned.
  -match string
    	SCAN MATCH option.
  -port int
    	Redis server port number. (default 6379)
  -prefixes string
    	You may specify custom prefixes (comma-separated).
  -separator string
    	Seperator for grouping. (default ":")
  -sleep int
    	Number of milliseconds to wait between reading keys.
  -timeout int
    	Milliseconds for timeout (default 3000)
  -top int
    	Only show the top number of prefixes.
```
