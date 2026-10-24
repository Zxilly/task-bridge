package model

// ValidateEnglishTaskPrompt applies the deterministic English-only input
// contract for new task submissions. Prompt byte limits remain the submitter's
// responsibility; stored tasks and idempotent retries need not be revalidated.
func ValidateEnglishTaskPrompt(prompt string) error {
	hasLetter := false
	for i := 0; i < len(prompt); i++ {
		char := prompt[i]
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') {
			hasLetter = true
		}
		if (char >= ' ' && char <= '~') || char == '\t' || char == '\n' || char == '\r' {
			continue
		}
		return Err("invalid_arguments", "prompt must be English only (ASCII): use printable ASCII plus TAB, LF or CR")
	}
	if !hasLetter {
		return Err("invalid_arguments", "prompt must be English only (ASCII) and contain at least one A-Z or a-z letter")
	}
	return nil
}
