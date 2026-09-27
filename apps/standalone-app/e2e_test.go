package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
)

func TestAgentLoopEndToEnd(t *testing.T) {
	toolServer := httptest.NewServer(http.HandlerFunc(echoTool))
	defer toolServer.Close()
	temporary := t.TempDir()
	app, err := newApplication(applicationOptions{
		databasePath: filepath.Join(temporary, "harness.sqlite"),
		artifactPath: filepath.Join(temporary, "artifacts"),
		toolEndpoint: toolServer.URL, logger: log.New(io.Discard, "", 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()
	server := httptest.NewServer(app.Handler())
	defer server.Close()

	chat := postJSON[domain.Chat](t, server.URL+"/v1/chats", map[string]string{"tenantId": "tenant-e2e"})
	postJSON[domain.Message](t, server.URL+"/v1/chats/"+chat.ChatID+"/messages", map[string]string{
		"tenantId": "tenant-e2e", "role": "user", "content": "请调用 echo 工具",
	})
	run := postJSON[domain.RunSnapshot](t, server.URL+"/v1/chats/"+chat.ChatID+"/runs", map[string]string{
		"tenantId": "tenant-e2e", "prompt": "请调用 echo 工具",
	})
	run = awaitTerminal(t, server.URL, run.RunID)
	if run.TerminalReason == nil || *run.TerminalReason != domain.TerminalCompleted {
		t.Fatalf("Run 未成功完成：%+v", run)
	}
	var runContext map[string]any
	if err := json.Unmarshal(run.Context, &runContext); err != nil {
		t.Fatal(err)
	}
	if runContext["assistant"] != "工具调用已完成。" {
		t.Fatalf("最终回复错误：%v", runContext["assistant"])
	}

	steps := getJSON[[]domain.Step](t, server.URL+"/v1/runs/"+run.RunID+"/steps", "tenant-e2e")
	if len(steps) != 4 {
		t.Fatalf("步骤数 = %d，期望 4；步骤：%+v", len(steps), steps)
	}
	effectEntries := getJSON[[]effects.EffectLedgerEntry](t, server.URL+"/v1/runs/"+run.RunID+"/effects", "tenant-e2e")
	if len(effectEntries) != 1 || effectEntries[0].Status != effects.EffectCommitted || effectEntries[0].ResultRef == "" {
		t.Fatalf("Effect Ledger 未收敛：%+v", effectEntries)
	}
	response := getResponse(t, server.URL+"/v1/runs/"+run.RunID+"/stream", "tenant-e2e")
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "event: harness/message.completed") {
		t.Fatalf("SSE 缺少最终消息事件：%s", body)
	}
}

func awaitTerminal(t *testing.T, baseURL, runID string) domain.RunSnapshot {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run := getJSON[domain.RunSnapshot](t, baseURL+"/v1/runs/"+runID, "tenant-e2e")
		if run.LifecycleStatus == domain.LifecycleTerminal {
			return run
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("等待 Run 终态超时")
	return domain.RunSnapshot{}
}

func postJSON[T any](t *testing.T, url string, input any) T {
	t.Helper()
	body, _ := json.Marshal(input)
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, _ := io.ReadAll(response.Body)
		t.Fatalf("POST %s 返回 %d：%s", url, response.StatusCode, data)
	}
	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func getJSON[T any](t *testing.T, url, tenantID string) T {
	t.Helper()
	response := getResponse(t, url, tenantID)
	defer response.Body.Close()
	var result T
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	return result
}

func getResponse(t *testing.T, url, tenantID string) *http.Response {
	t.Helper()
	request, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	request.Header.Set("X-Tenant-ID", tenantID)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		response.Body.Close()
		t.Fatalf("GET %s 返回 %d：%s", url, response.StatusCode, data)
	}
	return response
}
