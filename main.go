package main

import (
	"flag"
	"fmt"
	"github.com/go-redis/redis"
	"log"
	"gopkg.in/cheggaaa/pb.v1"
	"strings"
	"time"
	"sort"
)

var (
	flagHost     string
	flagPort     int
	flagDB       int
	flagMatch    string
	flagCount    int
	flagSleep    int
	flagLimit    int
	flagTop      int
	flagPrefixes string
	flagUseDump  bool
	flagTimeout  int
)

var (
	groupPrefixes []string
)

type prefixItems map[string]*prefixItem

func (items prefixItems) addKey(key string, bytes int) {
	prefix := getPrefix(key)

	if _, ok := items[prefix]; !ok {
		items[prefix] = &prefixItem{}
	}

	items[prefix].prefix = prefix
	items[prefix].bytes += bytes
	items[prefix].count += 1
}

func (items prefixItems) sortedSlice() []*prefixItem {
	// Pull all items out of the map
	slice := make([]*prefixItem, len(items))
	i := 0
	for _, item := range items {
		slice[i] = item
		i += 1
	}

	// Sort by size
	sort.Slice(slice, func(i, j int) bool {
		// Sort by "size desc, count desc"
		if slice[i].bytes == slice[j].bytes {
			return slice[i].count > slice[j].count
		}

		return slice[i].bytes > slice[j].bytes
	})

	return slice
}

type prefixItem struct {
	count  int
	bytes  int
	prefix string
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	parseCLIArgs()

	client := newClient()

	checkServerIsAlive(client)

	dbsize := getTotalKeys(client)
	if flagLimit > 0 && flagLimit < dbsize {
		dbsize = flagLimit
	}
	bar := pb.StartNew(dbsize)

	// Read keys
	cursor := uint64(0)
	var err error
	var keys []string

	prefixes := prefixItems{}

	for {
		keys, cursor, err = client.Scan(cursor, flagMatch, int64(flagCount)).Result()
		check(err)

		for _, key := range keys {
			bytes := 0
			if flagUseDump {
				result, err := client.Dump(key).Result()
				check(err)

				bytes = len(result)
			}

			prefixes.addKey(key, bytes)
		}

		bar.Add(len(keys))

		if cursor == 0 {
			break;
		}

		if flagLimit > 0 && bar.Get() >= int64(flagLimit) {
			break;
		}

		if flagSleep > 0 {
			time.Sleep(time.Duration(flagSleep) * time.Millisecond)
		}
	}

	// Since the number of items returned from a cursor is up to the count it's
	// possible for the progress bar position to be greater than the total (when
	// using -limit). So just neatly adjust for that...
	if bar.Get() > bar.Total {
		bar.SetTotal64(bar.Get())
	}

	bar.FinishPrint("")

	// Show results
	for i, data := range prefixes.sortedSlice() {
		if flagTop > 0 && i >= flagTop {
			break;
		}

		if data.bytes > 0 {
			fmt.Printf("%s -> %d keys, %d bytes\n", data.prefix, data.count, data.bytes)
		} else {
			fmt.Printf("%s -> %d keys\n", data.prefix, data.count)
		}
	}
}

func getPrefix(key string) string {
	if groupPrefixes == nil {
		if flagPrefixes == "" {
			groupPrefixes = []string{}
		} else {
			groupPrefixes = strings.Split(flagPrefixes, ",")
		}
	}

	for _, prefix := range groupPrefixes {
		if strings.HasPrefix(key, prefix) {
			return prefix + "*"
		}
	}

	separator := ":"
	parts := strings.Split(key, separator)
	return strings.Join(parts[:len(parts)-1], separator) + ":*"
}

func getTotalKeys(client *redis.Client) int {
	dbsize, err := client.DbSize().Result()
	check(err)

	return int(dbsize)
}

func checkServerIsAlive(client *redis.Client) {
	_, err := client.Ping().Result()
	check(err)
}

func newClient() *redis.Client {
	addr := fmt.Sprintf("%s:%d", flagHost, flagPort)

	return redis.NewClient(&redis.Options{
		Addr:        addr,
		DB:          flagDB,
		ReadTimeout: time.Duration(flagTimeout) * time.Millisecond,
	})
}

func parseCLIArgs() {
	flag.StringVar(&flagHost, "host", "localhost", "redis server host")
	flag.IntVar(&flagPort, "port", 6379, "redis server port number")
	flag.IntVar(&flagDB, "db", 0, "redis server database")
	flag.StringVar(&flagMatch, "match", "", "SCAN MATCH option")
	flag.IntVar(&flagCount, "count", 10, "SCAN COUNT option")
	flag.IntVar(&flagSleep, "sleep", 0, "number of milliseconds to wait between reading keys")
	flag.IntVar(&flagLimit, "limit", 0, "limit the number of keys scanned")
	flag.IntVar(&flagTop, "top", 0, "only show the top number of prefixes")
	flag.StringVar(&flagPrefixes, "prefixes", "", "known prefixes to group")
	flag.BoolVar(&flagUseDump, "use-dump", false, "use DUMP to get key sizes (much slower)")
	flag.IntVar(&flagTop, "timeout", 3000, "milliseconds for timeout")

	flag.Parse()
}
