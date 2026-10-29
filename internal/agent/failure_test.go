package agent

import (
	"fmt"
	"testing"
)

func TestLaunchOutcomeIgnoresOtherSessionsAndRecoveredErrors(t *testing.T) {
	outcome := &launchOutcome{}
	line := func(id string) []byte {
		return []byte(fmt.Sprintf(`{"type":"assistant","session_id":%q,"is_api_error_message":true,"error":"authentication_failed","api_error_status":403}`, id))
	}
	outcome.observe(line("other-session"), "current-session")
	if outcome.failure != nil {
		t.Fatal("other session supplied the failure")
	}
	outcome.observe(line("current-session"), "current-session")
	if outcome.failure == nil || outcome.failure.Code != "claude_authentication_failed" {
		t.Fatal("missing authentication failure")
	}
	outcome.observe([]byte(`{"type":"result","session_id":"current-session","is_error":true}`), "current-session")
	if outcome.failure.Code != "claude_authentication_failed" {
		t.Fatal("generic result erased the specific failure")
	}
	outcome.observe([]byte(`{"type":"result","session_id":"current-session","is_error":false}`), "current-session")
	if outcome.failure != nil {
		t.Fatal("successful CLI recovery retained the earlier error")
	}
}
