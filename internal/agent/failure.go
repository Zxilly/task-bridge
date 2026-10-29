package agent

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Zxilly/task-bridge/internal/model"
)

// Only fixed diagnostics cross the account/process boundary. Raw output stays
// in the controller log, where it can include account details or prompt text.
type launchFailure struct {
	public model.Error
	cause  error
}

func (f *launchFailure) Error() string { return f.cause.Error() }
func (f *launchFailure) Unwrap() error { return f.cause }

type launchOutcome struct{ failure *model.Error }

func (o *launchOutcome) observe(line []byte, sessionID string) {
	if o == nil {
		return
	}
	var event struct {
		Type      string `json:"type"`
		SessionID string `json:"session_id"`
		APIError  bool   `json:"is_api_error_message"`
		IsError   bool   `json:"is_error"`
		Error     string `json:"error"`
		Status    int    `json:"api_error_status"`
	}
	if json.Unmarshal(line, &event) != nil || event.SessionID != sessionID {
		return
	}
	if event.Type == "result" && !event.IsError {
		o.failure = nil
		return
	}
	if !(event.Type == "assistant" && event.APIError || event.Type == "result" && event.IsError) {
		return
	}
	code, message := "claude_process_failed", "Claude Code ended before completing the task. Inspect the A-side process log before requesting resume."
	switch {
	case event.Error == "authentication_failed" || event.Status == 401 || event.Status == 403:
		code, message = "claude_authentication_failed", "Claude Code authentication was rejected. Check the A-side account login and network access before requesting resume."
	case event.Error == "rate_limit" || event.Status == 429:
		code, message = "claude_rate_limited", "Claude Code reached an account rate limit. Check agents_usage and wait for the limit to reset before requesting resume."
	case event.Status >= 500 && event.Status <= 599:
		code, message = "claude_api_unavailable", "The Claude API returned a server error. Check service availability before requesting resume."
	default:
		if o.failure != nil {
			return
		} // A generic result must not erase an API diagnosis.
	}
	if event.Status >= 400 && event.Status <= 599 {
		message = fmt.Sprintf("HTTP %d: %s", event.Status, message)
	}
	o.failure = &model.Error{Code: code, Message: message}
}

func publicSessionFailure(err error) model.Error {
	var failure *launchFailure
	if errors.As(err, &failure) {
		return failure.public
	}
	return model.Error{Code: "claude_session_incomplete", Message: "The Claude session ended before a durable task result. Inspect task and command records and the A-side log before requesting resume."}
}
