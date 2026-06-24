// news-digest fetches RSS feeds, groups items by category, and posts a digest to Telegram.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mmcdole/gofeed"
)

type Feed struct {
	Category string `json:"category"`
	Name     string `json:"name"`
	URL      string `json:"url"`
}

type Item struct {
	Category  string
	Source    string
	Title     string
	Link      string
	Summary   string
	Published time.Time
}

type State struct {
	LastRun   time.Time `json:"last_run"`
	SeenLinks []string  `json:"seen_links"`
}

const (
	maxPerCategory = 8
	maxSummaryLen  = 200
	fallbackHours  = 24
	telegramMaxMsg = 4000
	fetchTimeout   = 20 * time.Second
	parallelFetch  = 8
)

// Preferred category order in the digest. Unknown categories are appended after.
var categoryOrder = []string{"AI/ML", "Data Engineering", "Blockchain/Crypto", "General Tech"}

func main() {
	feedsPath := flag.String("feeds", "feeds.json", "Path to feeds JSON")
	statePath := flag.String("state", filepath.Join(os.Getenv("HOME"), ".news_digest_state.json"), "Path to state file")
	dryRun := flag.Bool("dry-run", false, "Print digest to stdout instead of sending to Telegram")
	flag.Parse()

	token := os.Getenv("TELEGRAM_BOT_TOKEN")
	chatID := os.Getenv("TELEGRAM_CHAT_ID")
	if !*dryRun && (token == "" || chatID == "") {
		log.Fatal("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars, or pass -dry-run")
	}

	feeds, err := loadFeeds(*feedsPath)
	if err != nil {
		log.Fatalf("load feeds: %v", err)
	}

	state, _ := loadState(*statePath)
	since := state.LastRun
	if since.IsZero() {
		since = time.Now().Add(-time.Duration(fallbackHours) * time.Hour)
	}
	seen := make(map[string]bool, len(state.SeenLinks))
	for _, l := range state.SeenLinks {
		seen[l] = true
	}

	items := fetchAll(feeds, since, seen)
	if len(items) == 0 {
		log.Println("No new items.")
		return
	}

	grouped := groupAndTrim(items)
	msg := formatDigest(grouped)

	if *dryRun {
		fmt.Println(msg)
		return
	}
	if err := sendTelegram(token, chatID, msg); err != nil {
		log.Fatalf("send telegram: %v", err)
	}

	for _, it := range items {
		state.SeenLinks = append(state.SeenLinks, it.Link)
	}
	if n := len(state.SeenLinks); n > 1000 {
		state.SeenLinks = state.SeenLinks[n-1000:]
	}
	state.LastRun = time.Now()
	if err := saveState(*statePath, state); err != nil {
		log.Printf("save state: %v", err)
	}
	log.Printf("Sent digest with %d items.", len(items))
}

func loadFeeds(path string) ([]Feed, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var feeds []Feed
	return feeds, json.Unmarshal(b, &feeds)
}

func loadState(path string) (State, error) {
	var s State
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	return s, json.Unmarshal(b, &s)
}

func saveState(path string, s State) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0644)
}

func fetchAll(feeds []Feed, since time.Time, seen map[string]bool) []Item {
	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		out []Item
	)
	parser := gofeed.NewParser()
	parser.Client = &http.Client{Timeout: fetchTimeout}

	sem := make(chan struct{}, parallelFetch)
	for _, f := range feeds {
		f := f
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), fetchTimeout+5*time.Second)
			defer cancel()

			feed, err := parser.ParseURLWithContext(f.URL, ctx)
			if err != nil {
				log.Printf("WARN  %-25s  %v", f.Name, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			for _, e := range feed.Items {
				if e.Link == "" || seen[e.Link] {
					continue
				}
				var pub time.Time
				if e.PublishedParsed != nil {
					pub = *e.PublishedParsed
				} else if e.UpdatedParsed != nil {
					pub = *e.UpdatedParsed
				}
				if !pub.IsZero() && pub.Before(since) {
					continue
				}
				if pub.IsZero() {
					pub = time.Now()
				}
				out = append(out, Item{
					Category:  f.Category,
					Source:    f.Name,
					Title:     strings.TrimSpace(e.Title),
					Link:      e.Link,
					Summary:   cleanSummary(e.Description),
					Published: pub,
				})
				seen[e.Link] = true
			}
		}()
	}
	wg.Wait()
	return out
}

var (
	htmlTagRe = regexp.MustCompile(`<[^>]+>`)
	spacesRe  = regexp.MustCompile(`\s+`)
)

func cleanSummary(s string) string {
	if s == "" {
		return ""
	}
	s = htmlTagRe.ReplaceAllString(s, "")
	s = html.UnescapeString(s)
	s = spacesRe.ReplaceAllString(strings.TrimSpace(s), " ")
	if len(s) > maxSummaryLen {
		cut := strings.LastIndex(s[:maxSummaryLen], " ")
		if cut < 0 {
			cut = maxSummaryLen
		}
		s = s[:cut] + "…"
	}
	return s
}

func groupAndTrim(items []Item) map[string][]Item {
	g := map[string][]Item{}
	for _, it := range items {
		g[it.Category] = append(g[it.Category], it)
	}
	for cat, list := range g {
		sort.Slice(list, func(i, j int) bool { return list[i].Published.After(list[j].Published) })
		if len(list) > maxPerCategory {
			list = list[:maxPerCategory]
		}
		g[cat] = list
	}
	return g
}

func formatDigest(grouped map[string][]Item) string {
	var b strings.Builder
	fmt.Fprintf(&b, "<b>📰 Tech Digest — %s</b>\n", time.Now().Format("Mon 2 Jan, 15:04"))

	written := map[string]bool{}
	for _, cat := range categoryOrder {
		if items, ok := grouped[cat]; ok && len(items) > 0 {
			writeSection(&b, cat, items)
			written[cat] = true
		}
	}
	for cat, items := range grouped {
		if !written[cat] && len(items) > 0 {
			writeSection(&b, cat, items)
		}
	}
	return b.String()
}

func writeSection(b *strings.Builder, cat string, items []Item) {
	fmt.Fprintf(b, "\n<b>━━ %s ━━</b>\n", html.EscapeString(cat))
	for i, it := range items {
		fmt.Fprintf(b, "%d. <a href=\"%s\"><b>%s</b></a>\n",
			i+1, html.EscapeString(it.Link), html.EscapeString(it.Title))
		fmt.Fprintf(b, "   <i>%s</i>\n", html.EscapeString(it.Source))
		if it.Summary != "" {
			fmt.Fprintf(b, "   %s\n", html.EscapeString(it.Summary))
		}
	}
}

func sendTelegram(token, chatID, text string) error {
	endpoint := fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", token)
	for _, chunk := range splitMessage(text, telegramMaxMsg) {
		body := url.Values{}
		body.Set("chat_id", chatID)
		body.Set("text", chunk)
		body.Set("parse_mode", "HTML")
		body.Set("disable_web_page_preview", "true")

		resp, err := http.PostForm(endpoint, body)
		if err != nil {
			return err
		}
		bs, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			return fmt.Errorf("telegram %d: %s", resp.StatusCode, bs)
		}
	}
	return nil
}

// splitMessage cuts text at newline boundaries so each piece stays under max.
// Telegram's hard limit is 4096; we use 4000 for safety.
func splitMessage(text string, max int) []string {
	var chunks []string
	for len(text) > max {
		cut := strings.LastIndex(text[:max], "\n")
		if cut < 0 {
			cut = max
		}
		chunks = append(chunks, text[:cut])
		text = strings.TrimLeft(text[cut:], "\n")
	}
	if len(text) > 0 {
		chunks = append(chunks, text)
	}
	return chunks
}
