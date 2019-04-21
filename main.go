package main

import (
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"time"

	"github.com/mmcdole/gofeed"

	_ "github.com/lib/pq"
)

type feed struct {
	id        int64
	feedurl   string
	domain    string
	lastfetch string
}

func main() {
	db, err := sql.Open("postgres", "postgresql://maxroach@localhost:26257/bank?sslmode=disable")
	if err != nil {
		log.Fatal("error connecting to the database: ", err)
	}

	// Create the "accounts" table.
	if _, err := db.Exec(
		"CREATE TABLE IF NOT EXISTS feeds (id INT PRIMARY KEY, domain string, feedurl string, lastfetch TIMESTAMPTZ,lastHash string ,lastContent string)"); err != nil {
		log.Fatal(err)
	}

	// addFeeds(db)

	for true {
		renewFeedProces(db)
		time.Sleep(5 * time.Second)
	}
}

func addFeeds(db *sql.DB) {
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (1, 'https://rss.golem.de/rss.php?feed=RSS1.0', 'rss.golem.de', now())"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (2, 'https://www.wired.com/feed', 'www.wired.com', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (3, 'https://www.heise.de/rss/heise-atom.xml', 'www.heise.de', now())"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (4, 'https://www.planet3dnow.de/cms/feed/', 'www.planet3dnow.de', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (5, 'https://www.spiegel.de/index.rss', 'www.spiegel.de', now())"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (6, 'https://www.spiegel.de/schlagzeilen/index.rss', 'www.spiegel.de', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (7, 'http://blog.fefe.de/rss.xml?html', 'blog.fefe.de', now())"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (8, 'https://newsfeed.zeit.de/index', 'newsfeed.zeit.de', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (9, 'https://medium.com/feed/invironment/tagged/food', 'medium.com', now())"); err != nil {
		log.Fatal(err)
	}

	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (10, 'https://search.lifedots.org/?category_news=1&q=china&pageno=1&time_range=&language=en-US&format=rss', 'search.lifedots.org', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (11, 'https://www.eff.org/rss/updates.xml', 'www.eff.org', now())"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.Exec(
		"INSERT INTO feeds (id, feedurl, domain, lastfetch) VALUES (12, 'https://act.eff.org/action.atom', 'www.eff.org', now())"); err != nil {
		log.Fatal(err)
	}
}

func renewFeedProces(db *sql.DB) {
	feeds := getFeedsThatNeedRenew(db)
	iterateOverCurrentFeeds(db, feeds)
}

func getFeedsThatNeedRenew(db *sql.DB) []feed {
	var urls []feed

	// Print out the balances.
	rows, err := db.Query("SELECT id, feedurl, domain, lastfetch FROM feeds WHERE AGE(lastfetch) >  INTERVAl '2 minutes';")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	fmt.Println("TICK")
	for rows.Next() {
		var id int64
		var feedurl, domain, lastfetch string
		if err := rows.Scan(&id, &feedurl, &domain, &lastfetch); err != nil {
			log.Fatal(err)
		}
		urls = append(urls, feed{id, feedurl, domain, lastfetch})
	}

	return urls
}

func iterateOverCurrentFeeds(db *sql.DB, feeds []feed) {
	for index := 0; index < len(feeds); index++ {
		go crawlFeed(db, feeds[index])
		// time.Sleep(100 * time.Millisecond)
	}
}

func crawlFeed(db *sql.DB, oneFeed feed) {
	timeGetTMP := time.Now()
	fp := gofeed.NewParser()
	feedData, _ := fp.ParseURL(oneFeed.feedurl)
	timeGet := time.Since(timeGetTMP)

	start := time.Now()

	e, err := json.Marshal(feedData)
	if err != nil {
		fmt.Println(err)
		return
	}

	h := md5.New()
	content := string(e)
	io.WriteString(h, content)
	hash := hex.EncodeToString(h.Sum(nil))
	if _, err := db.Exec(
		"UPDATE feeds SET (lastfetch ,lastHash ,lastContent) = (now(), $1 ,$2 ) WHERE id = $3", hash, content, strconv.FormatInt(oneFeed.id, 10)); err != nil {
		log.Fatal(err)
	}

	fmt.Println("\nTIME TO GET AND PARSE--> ", timeGet, " TIME TO hash and Insert:", oneFeed.domain, "-->", time.Since(start), "  HASH -->", hash)
}
