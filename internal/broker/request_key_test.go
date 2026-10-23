package broker

import (
	"bytes"
	"testing"

	"github.com/Zxilly/task-bridge/internal/model"
	"github.com/Zxilly/task-bridge/internal/storage"
)

func TestRequestKeysPreservePersistedHashEncoding(t *testing.T) {
	cases := []struct {
		name   string
		first  string
		second string
		timeMS int64
	}{
		{name: "zero"},
		{name: "ordinary", first: "printf hello", second: "subdir", timeMS: 120000},
		{name: "escaped_unicode", first: "<script>&\"\\\n\t仓颉\u2028\u2029", second: "目录/\"quoted\"", timeMS: 86400000},
		{name: "invalid_utf8", first: string([]byte{'a', 0xff, 0xfe}), second: string([]byte{0xc0, 0x80, 0}), timeMS: 1},
		{name: "large_integer", first: "workspace", second: "preserve integer precision", timeMS: 9223372036854775807},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// This is the exact positional representation used before typed keys.
			legacy := model.JSON([]any{tc.first, tc.second, tc.timeMS})
			encodings := [][]byte{
				model.JSON(commandRequestKey{Command: tc.first, Cwd: tc.second, TimeoutMS: tc.timeMS}),
				model.JSON(taskRequestKey{WorkspaceID: tc.first, Prompt: tc.second, DeadlineMS: tc.timeMS}),
			}
			for _, encoded := range encodings {
				if !bytes.Equal(encoded, legacy) {
					t.Fatalf("persisted idempotency representation changed: got %s, want %s", encoded, legacy)
				}
				if storage.Hash(string(encoded)) != storage.Hash(string(legacy)) {
					t.Fatal("persisted idempotency hash changed")
				}
			}
		})
	}
}
