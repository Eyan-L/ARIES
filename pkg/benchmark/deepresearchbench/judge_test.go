package deepresearchbench

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/hyscale-lab/aries/pkg/core"
)

// chatCompletionServer is a shared httptest fake OpenAI-compatible
// chat-completions endpoint, reused by judge_test.go, race_test.go, and
// fact_test.go: all three build on model.ChatClient.Chat, so they all exercise
// the same request/response shape.
func chatCompletionServer(t *testing.T, content string, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected path %q", r.URL.Path)
			http.Error(w, "unexpected path", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fake-key" {
			t.Errorf("unexpected Authorization header %q", r.Header.Get("Authorization"))
			http.Error(w, "unexpected authorization", http.StatusUnauthorized)
			return
		}
		w.WriteHeader(statusCode)
		response := map[string]any{
			"choices": []map[string]any{
				{"message": map[string]string{"content": content}},
			},
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
}

func TestJudgeClientGenerationSettings(t *testing.T) {
	temperature := 0.4
	for _, tc := range []struct {
		name, provider, effort string
		temperature            *float64
		maxTokens              int
		want                   map[string]any
	}{
		{name: "defaults", provider: "openai", want: map[string]any{"temperature": float64(0)}},
		{name: "deepseek off", provider: "deepseek", effort: "off", want: map[string]any{"temperature": float64(0), "thinking": map[string]any{"type": "disabled"}}},
		{name: "deepseek high", provider: "deepseek", effort: "high", want: map[string]any{"thinking": map[string]any{"type": "enabled"}, "reasoning_effort": "high"}},
		{name: "openai max with overrides", provider: "openai", effort: "max", temperature: &temperature, maxTokens: 4096, want: map[string]any{"reasoning_effort": "max", "temperature": 0.4, "max_tokens": float64(4096)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				if payload["model"] != "test-model" {
					t.Errorf("model = %v", payload["model"])
				}
				delete(payload, "model")
				delete(payload, "messages")
				if !reflect.DeepEqual(payload, tc.want) {
					t.Errorf("generation fields = %#v, want %#v", payload, tc.want)
				}
				_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"answer"}}]}`))
			}))
			defer server.Close()
			client, err := newJudgeClient(core.ModelConfig{Provider: tc.provider, BaseURL: server.URL + "/v1", APIKeyEnv: "TEST_KEY", Model: "test-model", ReasoningEffort: tc.effort, Temperature: tc.temperature, MaxTokens: tc.maxTokens}, func(string) ([]byte, bool) { return []byte("fake-key"), true })
			if err != nil {
				t.Fatal(err)
			}
			content, err := client.Chat(context.Background(), "system", "user")
			if err != nil || content != "answer" {
				t.Fatalf("chat = %q, %v", content, err)
			}
		})
	}
}
