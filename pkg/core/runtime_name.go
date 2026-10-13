package core

import "strings"

// runtimeTaskLimit keeps a runtime name within the 63-character DNS label that
// both a Kubernetes pod and a Docker network hostname require.
const runtimeTaskLimit = 24

// RuntimeName names one runtime as aries-<role>-<task>-<nonce>. The task part
// is only a readable hint: it is lowercased, reduced to letters, digits and
// hyphens, and truncated, so two tasks may share it. The nonce is what makes
// the name unique, and ownership labels carry the exact task ID.
func RuntimeName(role, taskID, nonce string) string {
	var task strings.Builder
	for _, character := range strings.ToLower(taskID) {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' {
			task.WriteRune(character)
		} else if task.Len() > 0 && !strings.HasSuffix(task.String(), "-") {
			task.WriteByte('-')
		}
		if task.Len() == runtimeTaskLimit {
			break
		}
	}
	hint := strings.TrimSuffix(task.String(), "-")
	if hint == "" {
		return "aries-" + role + "-" + nonce
	}
	return "aries-" + role + "-" + hint + "-" + nonce
}
