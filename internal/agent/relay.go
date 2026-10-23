package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Zxilly/task-bridge/internal/mcputil"
	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// taskRelay gives one Claude process access to one assignment over the
// controller's existing upstream session. It never reconnects that session.
type taskRelay struct {
	ctx      context.Context
	cancel   context.CancelFunc
	upstream *mcp.ClientSession
	scope    model.ChannelMetadata
	url      string
	token    string
	server   *http.Server
	served   chan error
	done     chan struct{}
	slots    chan struct{}

	mu        sync.Mutex
	revoked   bool
	inflight  sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

type relayRequestContextKey struct{}

func newRelay(ctx context.Context, upstream *mcp.ClientSession, offer model.ChannelNotification, tools []*mcp.Tool) (*taskRelay, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if upstream == nil || offer.Meta.TaskID == "" || offer.Meta.AssignmentID == "" {
		return nil, errors.New("relay requires an upstream session and task assignment")
	}
	if len(offer.Meta.TaskID) > 128 || len(offer.Meta.AssignmentID) > 128 {
		return nil, errors.New("relay task assignment exceeds identifier limit")
	}
	taskCtx, cancel := context.WithCancel(ctx)
	r := &taskRelay{
		ctx: taskCtx, cancel: cancel, upstream: upstream, scope: offer.Meta,
		token: rand.Text(), served: make(chan error, 1), done: make(chan struct{}), slots: make(chan struct{}, 64),
	}
	implementation := &mcp.Implementation{Name: "task-bridge-task", Version: model.Version}
	server := mcp.NewServer(implementation, nil)
	mcputil.AddResultBounds(server, implementation)
	seen := make(map[string]bool)
	for _, tool := range tools {
		if tool == nil || !relayAllowsTool(tool.Name) {
			continue
		}
		if seen[tool.Name] {
			cancel()
			return nil, fmt.Errorf("duplicate upstream tool %q", tool.Name)
		}
		// The upstream list is a wire boundary. Validate its schema shape before
		// AddTool, which panics on malformed schema definitions.
		data, err := json.Marshal(tool.InputSchema)
		var schema struct {
			Type string `json:"type"`
		}
		if err != nil || json.Unmarshal(data, &schema) != nil || schema.Type != "object" {
			cancel()
			return nil, fmt.Errorf("upstream tool %q has an invalid input schema", tool.Name)
		}
		copy := *tool
		server.AddTool(&copy, r.callTool)
		seen[tool.Name] = true
	}
	if len(seen) == 0 {
		cancel()
		return nil, errors.New("upstream provides no task tools")
	}
	handler := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, &mcp.StreamableHTTPOptions{
		Stateless: true, JSONResponse: true, MaxRequestBodyBytes: model.MaxMessageBytes,
		PropagateRequestCancellation: true,
	})
	protected := http.NewCrossOriginProtection().Handler(handler)
	r.server = &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if !r.enter() {
				http.Error(w, "task relay is closed", http.StatusServiceUnavailable)
				return
			}
			defer r.inflight.Done()
			if len(request.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(request.Header.Get("Authorization")), []byte("Bearer "+r.token)) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			if request.URL.Path != "/mcp" {
				http.NotFound(w, request)
				return
			}
			// Older SDK protocol versions detach request cancellation while
			// preserving values. Keep the HTTP lifetime available to callTool.
			requestCtx := context.WithValue(request.Context(), relayRequestContextKey{}, request.Context())
			protected.ServeHTTP(w, request.WithContext(requestCtx))
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("listen for task relay: %w", err)
	}
	r.url = "http://" + listener.Addr().String() + "/mcp"
	go func() { r.served <- r.server.Serve(listener) }()
	go func() {
		select {
		case <-taskCtx.Done():
			_ = r.Close()
		case <-r.done:
		}
	}()
	return r, nil
}

func relayAllowsTool(name string) bool {
	switch name {
	case "task_accept", "task_get", "bash", "command_get", "command_wait", "command_output", "command_cancel", "task_progress", "task_complete":
		return true
	default:
		return false
	}
}

func (r *taskRelay) URL() string   { return r.url }
func (r *taskRelay) Token() string { return r.token }

// enter prevents additions racing with Close's wait, including requests already
// accepted on an HTTP keep-alive connection when the assignment was revoked.
func (r *taskRelay) enter() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revoked || r.ctx.Err() != nil {
		return false
	}
	r.inflight.Add(1)
	return true
}

func (r *taskRelay) callTool(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	if !r.enter() {
		return mcputil.SmallError("relay_closed", "this task session is closed", r.scope.TaskID), nil
	}
	defer r.inflight.Done()
	select {
	case r.slots <- struct{}{}:
		defer func() { <-r.slots }()
	default:
		return mcputil.SmallError("too_many_requests", "at most 64 concurrent task tool calls", r.scope.TaskID), nil
	}
	if req.Params == nil || !relayAllowsTool(req.Params.Name) || !r.validArguments(req.Params.Arguments) {
		return mcputil.SmallError("invalid_arguments", "tool arguments must use this task and assignment", r.scope.TaskID), nil
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stopTask := context.AfterFunc(r.ctx, cancel)
	defer stopTask()
	if requestCtx, ok := ctx.Value(relayRequestContextKey{}).(context.Context); ok {
		stopRequest := context.AfterFunc(requestCtx, cancel)
		defer stopRequest()
	}
	if r.ctx.Err() != nil {
		cancel()
	}
	// Keep the original JSON arguments; decoding them through any would lose
	// large integers. MCP metadata belongs to each separate protocol session.
	result, err := r.upstream.CallTool(callCtx, &mcp.CallToolParams{Name: req.Params.Name, Arguments: req.Params.Arguments})
	var protocolError *jsonrpc.Error
	if errors.As(err, &protocolError) {
		// CallTool adds a Go error wrapper. Return the wire error itself so the
		// downstream SDK retains its original code, message and data.
		return nil, protocolError
	}
	return result, err
}

func (r *taskRelay) validArguments(raw json.RawMessage) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return false
	}
	seen := make(map[string]bool)
	var taskID string
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return false
		}
		key, ok := token.(string)
		if !ok || seen[key] {
			return false
		}
		seen[key] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return false
		}
		switch {
		case strings.EqualFold(key, "task_id"):
			if key != "task_id" || json.Unmarshal(value, &taskID) != nil || taskID != r.scope.TaskID {
				return false
			}
		case strings.EqualFold(key, "assignment_id"):
			var assignmentID string
			if key != "assignment_id" || json.Unmarshal(value, &assignmentID) != nil || assignmentID != r.scope.AssignmentID {
				return false
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return false
	}
	var extra json.RawMessage
	return taskID == r.scope.TaskID && decoder.Decode(&extra) == io.EOF
}

// Close revokes admission before cancelling calls. It joins both the listener
// and admitted HTTP/tool handlers but leaves the controller's upstream open.
func (r *taskRelay) Close() error {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.revoked = true
		r.mu.Unlock()
		r.cancel()
		r.closeErr = r.server.Close()
		if err := <-r.served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			r.closeErr = errors.Join(r.closeErr, err)
		}
		r.inflight.Wait()
		close(r.done)
	})
	return r.closeErr
}
