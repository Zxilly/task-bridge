package model

import (
	"strings"
	"testing"
)

func TestValidateEnglishTaskPrompt(t *testing.T) {
	for _, tc := range []struct {
		name, prompt string
		valid        bool
	}{
		{"sentence", "Inspect the workspace and run its tests.", true},
		{"multiline code", "Run:\r\n\tprintf '%s\\n' \"ok\"\nThen inspect /tmp/output-123.txt.", true},
		{"lowercase letter", "a", true},
		{"uppercase letter", "Z", true},
		{"all printable characters", " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~", true},
		{"Chinese", "检查工作区并运行测试", false},
		{"mixed Chinese", "Run tests 并报告结果", false},
		{"accented Latin", "Inspect the cafe\u0301 directory", false},
		{"curly punctuation", "Run the project's tests\u2014then report", false},
		{"emoji", "Run tests \U0001f680", false},
		{"nonbreaking space", "Run\u00a0tests", false},
		{"empty", "", false},
		{"only whitespace", " \t\r\n", false},
		{"only digits and punctuation", "1234 + 5678 = ?", false},
		{"NUL", "Run\x00tests", false},
		{"vertical tab", "Run\vtests", false},
		{"form feed", "Run\ftests", false},
		{"escape", "Run\x1b[0mtests", false},
		{"DEL", "Run\x7ftests", false},
		{"invalid UTF-8", "Run\xfftests", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateEnglishTaskPrompt(tc.prompt)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v, error=%v", tc.valid, err)
			}
			if err != nil {
				detail := AsError(err)
				if detail.Code != "invalid_arguments" || !strings.Contains(detail.Message, "English only (ASCII)") {
					t.Fatalf("unexpected validation error: %+v", detail)
				}
			}
		})
	}
}
