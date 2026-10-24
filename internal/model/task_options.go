package model

// ValidateTaskOptions checks launch options without pinning model availability
// to a broker-side catalog. Claude resolves aliases, provider IDs and access.
func ValidateTaskOptions(modelName, effort string) error {
	if len(modelName) > 1024 {
		return Err("invalid_arguments", "model must not exceed 1024 bytes")
	}
	for i := range len(modelName) {
		if modelName[i] <= 0x20 || modelName[i] > 0x7e || (i == 0 && modelName[i] == '-') {
			return Err("invalid_arguments", "model must be a printable ASCII identifier without spaces or a leading dash")
		}
	}
	switch effort {
	case "", "low", "medium", "high", "xhigh", "max":
		return nil
	default:
		return Err("invalid_arguments", "effort must be low, medium, high, xhigh, or max")
	}
}
