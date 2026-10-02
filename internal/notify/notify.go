// Package notify sends alert messages. Notifier is the only thing the rest of pg-bisync knows, so
// another channel (mail, chat, pager) is one more implementation.
package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	defaultTelegramURL = "https://api.telegram.org"
	requestTimeout     = 15 * time.Second
	maxErrorBody       = 200
)

// Notifier sends one text message.
type Notifier interface {
	Notify(ctx context.Context, text string) error
}

// Telegram sends messages through a Telegram bot.
type Telegram struct {
	Token   string // bot token (keep it out of config files: PGBISYNC_TELEGRAM_TOKEN)
	ChatID  string
	BaseURL string // default https://api.telegram.org (tests use a local server)
	Client  *http.Client
}

// Notify implements Notifier. Errors never contain the token.
func (t Telegram) Notify(ctx context.Context, text string) error {
	base := t.BaseURL
	if base == "" {
		base = defaultTelegramURL
	}
	form := url.Values{"chat_id": {t.ChatID}, "text": {text}, "disable_web_page_preview": {"true"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/bot"+t.Token+"/sendMessage", strings.NewReader(form.Encode()))
	if err != nil {
		return errors.New("telegram: build request failed")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return send(client(t.Client), req, "telegram")
}

// Webhook posts {"text": "..."} as JSON to a URL (Slack/Mattermost/Teams-compatible or own endpoint).
type Webhook struct {
	URL    string
	Client *http.Client
}

// Notify implements Notifier.
func (w Webhook) Notify(ctx context.Context, text string) error {
	body, _ := json.Marshal(map[string]string{"text": text})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return errors.New("webhook: build request failed")
	}
	req.Header.Set("Content-Type", "application/json")
	return send(client(w.Client), req, "webhook")
}

// Multi sends to every notifier and joins the errors.
type Multi []Notifier

// Notify implements Notifier.
func (m Multi) Notify(ctx context.Context, text string) error {
	var errs []error
	for _, n := range m {
		errs = append(errs, n.Notify(ctx, text))
	}
	return errors.Join(errs...)
}

func client(c *http.Client) *http.Client {
	if c != nil {
		return c
	}
	return &http.Client{Timeout: requestTimeout}
}

// send executes the request; on failure the error names the channel and status, not the URL.
func send(c *http.Client, req *http.Request, channel string) error {
	resp, err := c.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err // drop the URL (may contain a token)
		}
		return fmt.Errorf("%s: %w", channel, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
		return fmt.Errorf("%s: status %d: %s", channel, resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return nil
}
