package api

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	runtime "github.com/ai-sea/elastic-harness/components/onstate-runtime"
	"github.com/ai-sea/elastic-harness/core/domain"
	"github.com/ai-sea/elastic-harness/core/effects"
	"github.com/ai-sea/elastic-harness/core/ports"
	"github.com/ai-sea/elastic-harness/core/qname"
)

type Defaults struct {
	Harness domain.HarnessRef
	Profile domain.ProfileRef
	Budget  domain.Budget
}

type Server struct {
	store    ports.StateStore
	engine   *runtime.Engine
	ids      runtime.IDGenerator
	defaults Defaults
	now      func() time.Time
	cursors  *cursorSigner
	mux      *http.ServeMux
}

func New(store ports.StateStore, engine *runtime.Engine, ids runtime.IDGenerator, defaults Defaults, cursorKey []byte) *Server {
	server := &Server{
		store: store, engine: engine, ids: ids, defaults: defaults,
		now: func() time.Time { return time.Now().UTC() }, cursors: newCursorSigner(cursorKey), mux: http.NewServeMux(),
	}
	server.routes()
	return server
}

func (s *Server) Handler() http.Handler { return s.mux }

func (s *Server) routes() {
	s.mux.HandleFunc("POST /v1/chats", s.createChat)
	s.mux.HandleFunc("POST /v1/chats/{chatId}/messages", s.createMessage)
	s.mux.HandleFunc("POST /v1/chats/{chatId}/runs", s.createRun)
	// Go 1.22 net/http ServeMux 要求 wildcard 段以 '}' 结束,
// 不支持 "{runId}:cancel" 风格;Phase 1 把 §9.1 的 ":cancel" 改写为 "/cancel"
// (语义不变,架构文档已加注)。resume 未在 Phase 1 范围,暂未实现。
s.mux.HandleFunc("POST /v1/runs/{runId}/cancel", s.cancelRun)
	s.mux.HandleFunc("GET /v1/runs/{runId}", s.getRun)
	s.mux.HandleFunc("GET /v1/runs/{runId}/events", s.getEvents)
	s.mux.HandleFunc("GET /v1/runs/{runId}/steps", s.getSteps)
	s.mux.HandleFunc("GET /v1/runs/{runId}/effects", s.getEffects)
	s.mux.HandleFunc("GET /v1/runs/{runId}/inbox", s.getInbox)
	s.mux.HandleFunc("GET /v1/runs/{runId}/stream", s.streamRun)
}

func (s *Server) createChat(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TenantID string `json:"tenantId"`
	}
	if err := decode(request, &input); err != nil {
		writeError(response, http.StatusBadRequest, err)
		return
	}
	if input.TenantID == "" {
		writeError(response, http.StatusBadRequest, errors.New("tenantId 不能为空"))
		return
	}
	chat := domain.Chat{TenantID: input.TenantID, ChatID: s.ids.New("chat_"), CreatedAt: s.now()}
	if err := s.store.CreateChat(request.Context(), chat); err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, chat)
}

func (s *Server) createMessage(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TenantID string `json:"tenantId"`
		Role     string `json:"role"`
		Content  string `json:"content"`
	}
	if err := decode(request, &input); err != nil || input.TenantID == "" || input.Content == "" {
		writeError(response, http.StatusBadRequest, errors.New("tenantId 与 content 不能为空"))
		return
	}
	if input.Role == "" {
		input.Role = "user"
	}
	message := domain.Message{
		TenantID: input.TenantID, ChatID: request.PathValue("chatId"), MessageID: s.ids.New("msg_"),
		Role: input.Role, Content: input.Content, CreatedAt: s.now(),
	}
	if err := s.store.AppendMessage(request.Context(), message); err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusCreated, message)
}

func (s *Server) createRun(response http.ResponseWriter, request *http.Request) {
	var input struct {
		TenantID string `json:"tenantId"`
		Prompt   string `json:"prompt"`
	}
	if err := decode(request, &input); err != nil || input.TenantID == "" || input.Prompt == "" {
		writeError(response, http.StatusBadRequest, errors.New("tenantId 与 prompt 不能为空"))
		return
	}
	initialContext, _ := json.Marshal(map[string]string{"prompt": input.Prompt})
	run, err := s.engine.CreateRun(request.Context(), runtime.CreateRunRequest{
		TenantID: input.TenantID, ChatID: request.PathValue("chatId"), Harness: s.defaults.Harness,
		Profile: s.defaults.Profile, Budget: s.defaults.Budget, InitialContext: initialContext,
	})
	if err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, run)
}

func (s *Server) cancelRun(response http.ResponseWriter, request *http.Request) {
	runID := request.PathValue("runId")
	now := s.now()
	signal := effects.StateSignal{
		SignalID: s.ids.New("sig_"), RunID: runID, Type: qname.MustParse("harness/run.cancel.requested"),
		DedupeKey: runID + "/cancel", Priority: 1000, OccurredAt: now,
	}
	payload, _ := json.Marshal(map[string]string{"runId": runID, "dedupeKey": signal.DedupeKey})
	inserted, err := s.store.PutSignal(request.Context(), signal, ports.OutboxRecord{
		ID: s.ids.New("out_"), Channel: "state", Key: runID, Payload: payload, CreatedAt: now,
	})
	if err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusAccepted, map[string]bool{"accepted": inserted})
}

func (s *Server) getRun(response http.ResponseWriter, request *http.Request) {
	run, err := s.authorizedRun(request)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, run)
}

func (s *Server) getEvents(response http.ResponseWriter, request *http.Request) {
	run, err := s.authorizedRun(request)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	after, err := s.decodeCursor(request.URL.Query().Get("after"), run.RunID)
	if err != nil {
		writeError(response, http.StatusBadRequest, err)
		return
	}
	events, err := s.store.Events(request.Context(), run.RunID, after, 100)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, events)
}

func (s *Server) getSteps(response http.ResponseWriter, request *http.Request) {
	if _, err := s.authorizedRun(request); err != nil {
		writeStoreError(response, err)
		return
	}
	values, err := s.store.Steps(request.Context(), request.PathValue("runId"))
	writeQuery(response, values, err)
}

func (s *Server) getEffects(response http.ResponseWriter, request *http.Request) {
	if _, err := s.authorizedRun(request); err != nil {
		writeStoreError(response, err)
		return
	}
	values, err := s.store.Effects(request.Context(), request.PathValue("runId"))
	writeQuery(response, values, err)
}

func (s *Server) getInbox(response http.ResponseWriter, request *http.Request) {
	if _, err := s.authorizedRun(request); err != nil {
		writeStoreError(response, err)
		return
	}
	values, err := s.store.Inbox(request.Context(), request.PathValue("runId"))
	writeQuery(response, values, err)
}

func (s *Server) streamRun(response http.ResponseWriter, request *http.Request) {
	run, err := s.authorizedRun(request)
	if err != nil {
		writeStoreError(response, err)
		return
	}
	flusher, ok := response.(http.Flusher)
	if !ok {
		writeError(response, http.StatusInternalServerError, errors.New("响应不支持 SSE flush"))
		return
	}
	after, err := s.decodeCursor(request.URL.Query().Get("after"), run.RunID)
	if err != nil {
		writeError(response, http.StatusBadRequest, err)
		return
	}
	response.Header().Set("Content-Type", "text/event-stream")
	response.Header().Set("Cache-Control", "no-cache")
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := s.store.Events(request.Context(), run.RunID, after, 100)
		if err != nil {
			return
		}
		for _, event := range events {
			data, _ := json.Marshal(event)
			cursor := s.cursors.Sign(run.RunID, event.Sequence, s.now().Add(15*time.Minute))
			_, _ = fmt.Fprintf(response, "id: %s\nevent: %s\ndata: %s\n\n", cursor, event.EventType.String(), data)
			after = event.Sequence
		}
		if len(events) > 0 {
			flusher.Flush()
		}
		current, err := s.store.GetRun(request.Context(), run.RunID)
		if err != nil {
			return
		}
		if current.LifecycleStatus == domain.LifecycleTerminal && len(events) == 0 {
			return
		}
		select {
		case <-request.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) authorizedRun(request *http.Request) (domain.RunSnapshot, error) {
	run, err := s.store.GetRun(request.Context(), request.PathValue("runId"))
	if err != nil {
		return domain.RunSnapshot{}, err
	}
	tenantID := request.Header.Get("X-Tenant-ID")
	if tenantID == "" || tenantID != run.TenantID {
		return domain.RunSnapshot{}, ports.ErrUnauthorized
	}
	return run, nil
}

func (s *Server) decodeCursor(value, runID string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	return s.cursors.Verify(value, runID, s.now())
}

type cursorSigner struct{ key []byte }

func newCursorSigner(key []byte) *cursorSigner {
	if len(key) == 0 {
		digest := sha256.Sum256([]byte("standalone-development-cursor-key"))
		key = digest[:]
	}
	return &cursorSigner{key: append([]byte(nil), key...)}
}

func (s *cursorSigner) Sign(runID string, sequence int64, expires time.Time) string {
	payload := fmt.Sprintf("%s:%d:%d", runID, sequence, expires.Unix())
	signature := s.signature(payload)
	return base64.RawURLEncoding.EncodeToString([]byte(payload + ":" + signature))
}

func (s *cursorSigner) Verify(token, expectedRunID string, now time.Time) (int64, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return 0, errors.New("cursor 格式无效")
	}
	parts := strings.Split(string(decoded), ":")
	if len(parts) != 4 {
		return 0, errors.New("cursor 格式无效")
	}
	payload := strings.Join(parts[:3], ":")
	if !hmac.Equal([]byte(parts[3]), []byte(s.signature(payload))) || parts[0] != expectedRunID {
		return 0, errors.New("cursor 签名无效")
	}
	sequence, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, errors.New("cursor sequence 无效")
	}
	expires, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || now.Unix() >= expires {
		return 0, errors.New("cursor 已过期")
	}
	return sequence, nil
}

func (s *cursorSigner) signature(payload string) string {
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func decode(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func writeQuery(response http.ResponseWriter, value any, err error) {
	if err != nil {
		writeStoreError(response, err)
		return
	}
	writeJSON(response, http.StatusOK, value)
}

func writeStoreError(response http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ports.ErrNotFound):
		writeError(response, http.StatusNotFound, err)
	case errors.Is(err, ports.ErrConflict), errors.Is(err, ports.ErrTerminal):
		writeError(response, http.StatusConflict, err)
	case errors.Is(err, ports.ErrUnauthorized):
		writeError(response, http.StatusForbidden, err)
	default:
		writeError(response, http.StatusInternalServerError, err)
	}
}

func writeError(response http.ResponseWriter, status int, err error) {
	writeJSON(response, status, map[string]string{"error": err.Error()})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}
