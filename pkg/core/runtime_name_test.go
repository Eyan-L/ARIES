package core

import (
	"regexp"
	"strings"
	"testing"
)

func TestRuntimeNameIsAReadableDNSLabel(t *testing.T) {
	const nonce = "03cfd3ce39bab2f8"
	for taskID, want := range map[string]string{
		"fix-git":     "aries-sandbox-fix-git-" + nonce,
		"fix-git-001": "aries-sandbox-fix-git-001-" + nonce,
		// Uppercase, underscores and dots are not valid in a pod name.
		"Instance_Django__django.12345": "aries-sandbox-instance-django-django-1-" + nonce,
		"---":                           "aries-sandbox-" + nonce,
		"":                              "aries-sandbox-" + nonce,
	} {
		if got := RuntimeName("sandbox", taskID, nonce); got != want {
			t.Errorf("RuntimeName(%q) = %q, want %q", taskID, got, want)
		}
	}
	label := regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	longest := RuntimeName("openclaw", strings.Repeat("Task_", 30), nonce)
	if !label.MatchString(longest) || len(longest) > 63 {
		t.Fatalf("name %q (%d characters) is not a DNS label", longest, len(longest))
	}
	// The nonce, not the truncated task, keeps names distinct.
	if RuntimeName("sandbox", "fix-git", "aaaa") == RuntimeName("sandbox", "fix-git", "bbbb") {
		t.Fatal("names with different nonces collide")
	}
}
