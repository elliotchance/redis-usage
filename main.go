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
	flagHost      string
	flagPort      int
    flagPassword  string
	flagDB        int
	flagMatch     string
	flagCount     int
	flagSleep     int
	flagLimit     int
	flagTop       int
	flagPrefixes  string
	flagDumpLimit int
	flagTimeout   int
	flagSeparator string
)

var (
	groupPrefixes []string
	prefixes      prefixItems
)

type prefixItems map[string]*prefixItem

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
		if slice[i].estimatedSize() == slice[j].estimatedSize() {
			return slice[i].count > slice[j].count
		}

		return slice[i].estimatedSize() > slice[j].estimatedSize()
	})

	return slice
}

type prefixItem struct {
	count         int
	totalBytes    int
	numberOfDumps int
	prefix        string
}

func (item prefixItem) averageBytesPerKey() float64 {
	if item.numberOfDumps == 0 {
		return 0
	}

	return float64(item.totalBytes) / float64(item.numberOfDumps)
}

func (item prefixItem) estimatedSize() int64 {
	return int64(item.averageBytesPerKey() * float64(item.count))
}

func formatSize(size int64) string {
	switch {
	case size < 1024:
		return fmt.Sprintf("%d bytes", size)
	case size < 1024 * 1024:
		return fmt.Sprintf("%.3g KB", float64(size) / 1024)
	case size < 1024 * 1024 * 1024:
		return fmt.Sprintf("%.3g MB", float64(size) / (1024 * 1024))
	default:
		return fmt.Sprintf("%.3g GB", float64(size) / (1024 * 1024 * 1024))
	}
}

func check(err error) {
	if err != nil {
		printResults()
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

	prefixes = prefixItems{}

	for {
		keys, cursor, err = client.Scan(cursor, flagMatch, int64(flagCount)).Result()
		check(err)

		for _, key := range keys {
			prefix := getPrefix(key)

			if _, ok := prefixes[prefix]; !ok {
				prefixes[prefix] = &prefixItem{}
			}

			prefixes[prefix].prefix = prefix
			prefixes[prefix].count += 1

			if prefixes[prefix].numberOfDumps < flagDumpLimit {
				result, err := client.Dump(key).Result()
				check(err)

				prefixes[prefix].totalBytes += len(result)
				prefixes[prefix].numberOfDumps += 1
			}
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

	printResults()
}

func printResults() {
	if prefixes == nil {
		return
	}

	for i, data := range prefixes.sortedSlice() {
		if flagTop > 0 && i >= flagTop {
			break;
		}

		if data.numberOfDumps > 0 {
			fmt.Printf("%s -> %d keys, ~%s estimated size\n", data.prefix,
				data.count, formatSize(data.estimatedSize()))
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

	parts := strings.Split(key, flagSeparator)

	return strings.Join(parts[:len(parts)-1], flagSeparator) + flagSeparator + "*"
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
        Password:    flagPassword,
		DB:          flagDB,
		ReadTimeout: time.Duration(flagTimeout) * time.Millisecond,
	})
}

func parseCLIArgs() {
	flag.StringVar(&flagHost, "host", "localhost", "Redis server host.")
	flag.StringVar(&flagPassword, "password", "", "Redis server password.")
	flag.IntVar(&flagPort, "port", 6379, "Redis server port number.")
	flag.IntVar(&flagDB, "db", 0, "Redis server database.")
	flag.StringVar(&flagMatch, "match", "", "SCAN MATCH option.")
	flag.IntVar(&flagCount, "count", 10, "SCAN COUNT option.")
	flag.IntVar(&flagSleep, "sleep", 0, "Number of milliseconds to wait " +
		"between reading keys.")
	flag.IntVar(&flagLimit, "limit", 0, "Limit the number of keys scanned.")
	flag.IntVar(&flagTop, "top", 0, "Only show the top number of prefixes.")
	flag.StringVar(&flagPrefixes, "prefixes", "", "You may specify custom " +
		"prefixes (comma-separated).")
	flag.IntVar(&flagDumpLimit, "dump-limit", 0, "Use DUMP to get key sizes "+
		"(much slower). If this is zero then DUMP will not be used, "+
		"otherwise it will take N sizes for each prefix to calculate an "+
		"average bytes for that key prefix. If you want to measure the sizes "+
		"for all keys set this to a very large number.")
	flag.IntVar(&flagTop, "timeout", 3000, "Milliseconds for timeout")
	flag.StringVar(&flagSeparator, "separator", ":", "Seperator for grouping.")

	flag.Parse()
}
