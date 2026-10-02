package notify

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTelegram(t *testing.T) {
	var gotPath, gotChat, gotText string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = r.ParseForm()
		gotChat, gotText = r.PostForm.Get("chat_id"), r.PostForm.Get("text")
		if strings.Contains(r.URL.Path, "bad") {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"ok":false,"description":"Unauthorized"}`)
		}
	}))
	defer srv.Close()

	t.Run("N1_sends_form", func(t *testing.T) {
		err := Telegram{Token: "123:abc", ChatID: "-42", BaseURL: srv.URL}.Notify(context.Background(), "hello ü")
		if err != nil || gotPath != "/bot123:abc/sendMessage" || gotChat != "-42" || gotText != "hello ü" {
			t.Fatalf("err=%v path=%s chat=%s text=%s", err, gotPath, gotChat, gotText)
		}
	})
	t.Run("N2_error_without_token", func(t *testing.T) {
		err := Telegram{Token: "bad-secret-token", ChatID: "1", BaseURL: srv.URL}.Notify(context.Background(), "x")
		if err == nil || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "401") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("N3_unreachable_without_token", func(t *testing.T) {
		err := Telegram{Token: "secret-token", ChatID: "1", BaseURL: "http://127.0.0.1:1"}.Notify(context.Background(), "x")
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestWebhookAndMulti(t *testing.T) {
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var b map[string]string
		_ = json.NewDecoder(r.Body).Decode(&b)
		got = append(got, b["text"])
	}))
	defer srv.Close()
	m := Multi{Webhook{URL: srv.URL}, Webhook{URL: srv.URL}}
	if err := m.Notify(context.Background(), "x"); err != nil || len(got) != 2 || got[0] != "x" {
		t.Fatalf("err=%v got=%v", err, got)
	}
	m = Multi{Webhook{URL: srv.URL}, Webhook{URL: "http://127.0.0.1:1"}}
	if err := m.Notify(context.Background(), "y"); err == nil || len(got) != 3 {
		t.Fatalf("one failing notifier must not stop the others: err=%v got=%v", err, got)
	}
}
