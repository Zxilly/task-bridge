package server

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func TestSetupGuidesArePublicStaticMarkdown(t *testing.T) {
	// Documentation does not require broker storage, device credentials, or a
	// trusted proxy header to construct its contents.
	handler := NewHandler(nil)
	for _, guide := range []struct {
		path     string
		contains []string
	}{
		{"/setup/claude.md", []string{"Claude Code", "/mcp/agent", "BRIDGE_AGENT_TOKEN"}},
		{"/setup/client.md", []string{"task-bridge-client", "/v1/client", "doctor"}},
	} {
		t.Run(guide.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, guide.path, nil)
			request.Host = "untrusted-host.invalid"
			request.Header.Set("Forwarded", "host=untrusted-forwarded.invalid;proto=https")
			request.Header.Set("X-Forwarded-Host", "untrusted-forwarded.invalid")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "text/markdown; charset=utf-8" {
				t.Fatalf("guide response: status=%d headers=%v", response.Code, response.Header())
			}
			body := response.Body.String()
			for _, text := range guide.contains {
				if !strings.Contains(body, text) {
					t.Errorf("guide missing %q", text)
				}
			}
			if strings.Contains(body, "untrusted-host.invalid") || strings.Contains(body, "untrusted-forwarded.invalid") {
				t.Fatal("request headers were interpolated into executable setup instructions")
			}
			if response.Header().Get("Cache-Control") != "no-cache" {
				t.Fatal("guide may remain stale after a service upgrade")
			}
			head := httptest.NewRecorder()
			handler.ServeHTTP(head, httptest.NewRequest(http.MethodHead, guide.path, nil))
			if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Content-Length") != strconv.Itoa(len(body)) {
				t.Fatalf("HEAD did not describe the guide: status=%d headers=%v body=%d", head.Code, head.Header(), head.Body.Len())
			}
			post := httptest.NewRecorder()
			handler.ServeHTTP(post, httptest.NewRequest(http.MethodPost, guide.path, nil))
			if post.Code != http.StatusMethodNotAllowed {
				t.Fatalf("POST status=%d", post.Code)
			}
		})
	}
	for _, path := range []string{"/setup/", "/setup/unknown.md", "/setup/claude.md/extra"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Fatalf("unregistered guide %q status=%d", path, response.Code)
		}
	}
}
