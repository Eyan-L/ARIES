package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The host install script cannot read the catalog, so its pin must match it.
func TestVMDInstallScriptMatchesCatalog(t *testing.T) {
	file, err := os.Open(filepath.Join("..", "..", "configs", "versions.json"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	versions, err := DecodeVersions(file)
	if err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(filepath.Join("..", "..", "scripts", "vmd", "install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	kata := versions.VMD.Kata
	for _, line := range []string{
		"KATA_VERSION=" + kata.Version,
		"KATA_SHA256=" + kata.SHA256,
	} {
		if !strings.Contains(string(script), "\n"+line+"\n") {
			t.Errorf("scripts/vmd/install.sh lacks %q from configs/versions.json", line)
		}
	}
	expandedURL := strings.ReplaceAll(scriptValue(string(script), "KATA_URL"), "${KATA_VERSION}", kata.Version)
	if expandedURL != kata.URL {
		t.Errorf("install.sh KATA_URL expands to %q, want %q", expandedURL, kata.URL)
	}
}

func scriptValue(script, name string) string {
	for _, line := range strings.Split(script, "\n") {
		if value, ok := strings.CutPrefix(line, name+"="); ok {
			return value
		}
	}
	return ""
}
