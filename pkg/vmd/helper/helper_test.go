package helper

import (
	"archive/tar"
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

const helperEnv = "ARIES_VMD_HELPER_TEST_MAIN"

// TestMain lets tests run the real helper by re-executing the test binary.
func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) == "1" {
		os.Exit(Default().Main(os.Args[1:]))
	}
	os.Exit(m.Run())
}

func helperCommand(args ...string) *exec.Cmd {
	command := exec.Command(os.Args[0], args...)
	command.Env = append(os.Environ(), helperEnv+"=1")
	return command
}

func exitCode(t *testing.T, err error) int {
	t.Helper()
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("helper did not exit normally: %v", err)
	}
	return exitErr.ExitCode()
}

func TestSharedContract(t *testing.T) {
	pairs := [][2]int{
		{ExitNotFound, api.HelperExitNotFound},
		{ExitUsage, api.HelperExitUsage},
		{ExitStateMissing, api.HelperExitStateMissing},
		{ExitUnconfirmed, api.HelperExitUnconfirmed},
	}
	for _, pair := range pairs {
		if pair[0] != pair[1] {
			t.Fatalf("helper exit code %d differs from api %d", pair[0], pair[1])
		}
	}
	local, shared := reflect.TypeOf(FileStat{}), reflect.TypeOf(api.FileStat{})
	if local.NumField() != shared.NumField() {
		t.Fatal("FileStat field count differs from api.FileStat")
	}
	for i := range local.NumField() {
		a, b := local.Field(i), shared.Field(i)
		if a.Name != b.Name || a.Type != b.Type || a.Tag != b.Tag {
			t.Fatalf("FileStat field %s differs from api.FileStat", a.Name)
		}
	}
}

func TestVersion(t *testing.T) {
	output, err := helperCommand("version").Output()
	if err != nil || strings.TrimSpace(string(output)) != Version {
		t.Fatalf("version = %q, %v", output, err)
	}
	if code := exitCode(t, helperCommand("bogus").Run()); code != ExitUsage {
		t.Fatalf("unknown subcommand exit = %d", code)
	}
}

func TestRunExitCodes(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	cases := []struct {
		argv []string
		want int
	}{
		{[]string{"sh", "-c", "exit 7"}, 7},
		{[]string{"sh", "-c", "kill -TERM $$"}, 128 + int(syscall.SIGTERM)},
		{[]string{"aries-no-such-command"}, 127},
		{[]string{"/nonexistent/binary"}, 127},
		{[]string{t.TempDir()}, 126},
		{[]string{"true"}, 0},
	}
	for _, tc := range cases {
		args := append([]string{"run", "--state", state, "--"}, tc.argv...)
		if code := exitCode(t, helperCommand(args...).Run()); code != tc.want {
			t.Errorf("%v exit = %d, want %d", tc.argv, code, tc.want)
		}
		if _, err := os.Stat(state); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("%v left state file: %v", tc.argv, err)
		}
	}
	if code := exitCode(t, helperCommand("run", "--state", "relative", "--", "true").Run()); code != ExitUsage {
		t.Fatalf("relative state exit = %d", code)
	}
}

func TestRunPassesStdio(t *testing.T) {
	command := helperCommand("run", "--state", filepath.Join(t.TempDir(), "state"), "--", "sh", "-c", "cat; echo err >&2")
	command.Stdin = strings.NewReader("input")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "input" || stderr.String() != "err\n" {
		t.Fatalf("stdout %q stderr %q", stdout.String(), stderr.String())
	}
}

func TestKillTerminatesProcessGroup(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	run := helperCommand("run", "--state", state, "--", "sh", "-c", "sleep 100 & sleep 100")
	if err := run.Start(); err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- run.Wait() }()
	content, err := waitForFile(state, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	pgid, err := strconv.Atoi(strings.TrimSpace(string(content)))
	if err != nil {
		t.Fatal(err)
	}
	// Wait for the background sleep so the group has two members.
	deadline := time.Now().Add(5 * time.Second)
	for {
		table, err := processes("/proc")
		if err != nil {
			t.Fatal(err)
		}
		members := 0
		for _, stat := range table {
			if stat.PGRP == pgid {
				members++
			}
		}
		if members >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("process group has %d members", members)
		}
		time.Sleep(pollInterval)
	}
	var stderr bytes.Buffer
	kill := helperCommand("kill", "--state", state, "--grace", "50ms")
	kill.Stderr = &stderr
	if code := exitCode(t, kill.Run()); code != 0 {
		t.Fatalf("kill exit = %d: %s", code, stderr.String())
	}
	if present, err := groupPresent("/proc", pgid); err != nil || present {
		t.Fatalf("group present = %v, %v", present, err)
	}
	select {
	case err := <-runDone:
		if code := exitCode(t, err); code != 128+int(syscall.SIGTERM) && code != 128+int(syscall.SIGKILL) {
			t.Fatalf("run exit = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not exit after kill")
	}
	if _, err := os.Stat(state); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("state file remains: %v", err)
	}
}

func TestKillMissingState(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if code := exitCode(t, helperCommand("kill", "--state", state, "--wait", "50ms").Run()); code != ExitStateMissing {
		t.Fatalf("missing state exit = %d", code)
	}
	for _, content := range []string{"1\n", "0\n", "abc\n", "-5\n"} {
		if err := os.WriteFile(state, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if code := exitCode(t, helperCommand("kill", "--state", state).Run()); code != ExitUnconfirmed {
			t.Fatalf("state %q exit = %d", content, code)
		}
	}
}

func TestParseStat(t *testing.T) {
	stat, err := parseStat("123 (we ird) (name)) S 1 456 456 0 -1 4194560")
	if err != nil {
		t.Fatal(err)
	}
	if stat != (procStat{State: 'S', PPID: 1, PGRP: 456}) {
		t.Fatalf("stat = %+v", stat)
	}
	for _, bad := range []string{"", "123 (x", "123 (x) S", "123 (x) S a 4", "123 (x) S 1 b"} {
		if _, err := parseStat(bad); err == nil {
			t.Errorf("parseStat(%q) succeeded", bad)
		}
	}
}

func TestGroupPresentIgnoresZombies(t *testing.T) {
	root := t.TempDir()
	write := func(pid, content string) {
		if err := os.MkdirAll(filepath.Join(root, pid), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, pid, "stat"), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("10", "10 (sh) Z 1 10 10")
	write("11", "11 (sleep) S 1 20 20")
	if err := os.MkdirAll(filepath.Join(root, "self"), 0o700); err != nil {
		t.Fatal(err)
	}
	if present, err := groupPresent(root, 10); err != nil || present {
		t.Fatalf("zombie group present = %v, %v", present, err)
	}
	if present, err := groupPresent(root, 20); err != nil || !present {
		t.Fatalf("live group present = %v, %v", present, err)
	}
}

func TestInitReapsOrphans(t *testing.T) {
	// The subshell exits at once, orphaning a short sleep that reparents to init.
	command := helperCommand("init", "--", "sh", "-c", "(sleep 0.3 &); sleep 2; exit 3")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	initPID := command.Process.Pid
	children := func() map[int]procStat {
		table, err := processes("/proc")
		if err != nil {
			t.Fatal(err)
		}
		result := map[int]procStat{}
		for pid, stat := range table {
			if stat.PPID == initPID {
				result[pid] = stat
			}
		}
		return result
	}
	adopted := false
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		current := children()
		if len(current) >= 2 {
			adopted = true
		}
		if adopted && len(current) == 1 {
			break
		}
		time.Sleep(pollInterval)
	}
	if !adopted {
		t.Fatal("orphan was not adopted by init")
	}
	for pid, stat := range children() {
		if stat.State == 'Z' {
			t.Fatalf("init left zombie %d", pid)
		}
	}
	if len(children()) != 1 {
		t.Fatal("orphan was not reaped")
	}
	select {
	case err := <-done:
		if code := exitCode(t, err); code != 3 {
			t.Fatalf("init exit = %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("init did not exit")
	}
}

func TestInitForwardsSignals(t *testing.T) {
	command := helperCommand("init", "--", "sleep", "100")
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := exitCode(t, command.Wait()); code != 128+int(syscall.SIGTERM) {
		t.Fatalf("init exit = %d", code)
	}
}

type entry struct {
	header  tar.Header
	content string
}

func archive(t *testing.T, entries ...entry) *bytes.Buffer {
	t.Helper()
	var buffer bytes.Buffer
	writer := tar.NewWriter(&buffer)
	for _, e := range entries {
		header := e.header
		header.Size = int64(len(e.content))
		if header.Uid == 0 && header.Gid == 0 {
			header.Uid, header.Gid = os.Getuid(), os.Getgid()
		}
		if err := writer.WriteHeader(&header); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(e.content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return &buffer
}

func runExtract(t *testing.T, input io.Reader, args ...string) (int, string) {
	t.Helper()
	var stderr bytes.Buffer
	h := &Helper{Stdin: input, Stdout: io.Discard, Stderr: &stderr, ProcRoot: "/proc", Root: "/"}
	return h.Main(append([]string{"tar-extract"}, args...)), stderr.String()
}

func TestTarExtractPreservesModesAndTimes(t *testing.T) {
	dir := t.TempDir()
	mtime := time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)
	input := archive(t,
		entry{header: tar.Header{Name: "d/", Typeflag: tar.TypeDir, Mode: 0o1750, ModTime: mtime}},
		entry{header: tar.Header{Name: "d/run", Typeflag: tar.TypeReg, Mode: 0o4755, ModTime: mtime}, content: "#!/bin/sh\n"},
		entry{header: tar.Header{Name: "d/link", Typeflag: tar.TypeSymlink, Linkname: "run", ModTime: mtime}},
		entry{header: tar.Header{Name: "d/hard", Typeflag: tar.TypeLink, Linkname: "d/run", ModTime: mtime}},
		entry{header: tar.Header{Name: "new/parent/file", Typeflag: tar.TypeReg, Mode: 0o600, ModTime: mtime}, content: "x"},
	)
	if code, stderr := runExtract(t, input, dir); code != 0 {
		t.Fatalf("extract exit %d: %s", code, stderr)
	}
	info, err := os.Stat(filepath.Join(dir, "d/run"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode() != fs.ModeSetuid|0o755 || !info.ModTime().Equal(mtime) {
		t.Fatalf("file mode %v mtime %v", info.Mode(), info.ModTime())
	}
	directory, err := os.Stat(filepath.Join(dir, "d"))
	if err != nil {
		t.Fatal(err)
	}
	if directory.Mode() != fs.ModeDir|fs.ModeSticky|0o750 || !directory.ModTime().Equal(mtime) {
		t.Fatalf("dir mode %v mtime %v", directory.Mode(), directory.ModTime())
	}
	if target, err := os.Readlink(filepath.Join(dir, "d/link")); err != nil || target != "run" {
		t.Fatalf("symlink = %q, %v", target, err)
	}
	hard, err := os.Stat(filepath.Join(dir, "d/hard"))
	if err != nil || !os.SameFile(hard, info) {
		t.Fatalf("hardlink not linked: %v", err)
	}
	if content, err := os.ReadFile(filepath.Join(dir, "new/parent/file")); err != nil || string(content) != "x" {
		t.Fatalf("nested file = %q, %v", content, err)
	}
}

func TestTarExtractOverwrites(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "f"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "full/child"), 0o755); err != nil {
		t.Fatal(err)
	}
	if code, stderr := runExtract(t, archive(t, entry{header: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644}, content: "new"}), dir); code != 0 {
		t.Fatalf("overwrite exit %d: %s", code, stderr)
	}
	if content, _ := os.ReadFile(filepath.Join(dir, "f")); string(content) != "new" {
		t.Fatalf("content = %q", content)
	}
	if code, _ := runExtract(t, archive(t, entry{header: tar.Header{Name: "full", Typeflag: tar.TypeReg, Mode: 0o644}, content: "x"}), dir); code != exitFailure {
		t.Fatalf("replacing non-empty dir exit = %d", code)
	}
}

func TestTarExtractRejectsEscapes(t *testing.T) {
	outside := t.TempDir()
	cases := map[string]*bytes.Buffer{
		"absolute":  archive(t, entry{header: tar.Header{Name: "/etc/x", Typeflag: tar.TypeReg, Mode: 0o644}}),
		"parent":    archive(t, entry{header: tar.Header{Name: "a/../../x", Typeflag: tar.TypeReg, Mode: 0o644}}),
		"hardlink":  archive(t, entry{header: tar.Header{Name: "h", Typeflag: tar.TypeLink, Linkname: "../x"}}),
		"via-link":  archive(t, entry{header: tar.Header{Name: "out", Typeflag: tar.TypeSymlink, Linkname: outside}}, entry{header: tar.Header{Name: "out/x", Typeflag: tar.TypeReg, Mode: 0o644}, content: "x"}),
		"unsupport": archive(t, entry{header: tar.Header{Name: "g", Typeflag: 'Z'}}),
	}
	for name, input := range cases {
		dir := t.TempDir()
		if code, _ := runExtract(t, input, dir); code != exitFailure {
			t.Errorf("%s: exit = %d", name, code)
		}
	}
	if entries, _ := os.ReadDir(outside); len(entries) != 0 {
		t.Fatalf("extraction escaped into %s", outside)
	}
}

func TestTarExtractMissingDestination(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, destination := range []string{filepath.Join(dir, "missing"), file} {
		if code, _ := runExtract(t, archive(t), destination); code != ExitNotFound {
			t.Errorf("%s exit = %d", destination, code)
		}
	}
	if code, _ := runExtract(t, archive(t), "relative"); code != ExitUsage {
		t.Fatalf("relative destination exit = %d", code)
	}
}

func TestTarExtractOwnerUser(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0o755); err != nil {
		t.Fatal(err)
	}
	uid, gid := os.Getuid(), os.Getgid()
	passwd := "self:x:" + strconv.Itoa(uid) + ":" + strconv.Itoa(gid) + "::/:/bin/sh\n"
	if err := os.WriteFile(filepath.Join(root, "etc/passwd"), []byte(passwd), 0o644); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	h := &Helper{Stdin: archive(t, entry{header: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 12345, Gid: 12345}}), Stdout: io.Discard, Stderr: io.Discard, Root: root}
	if code := h.Main([]string{"tar-extract", "--owner-user", "self", dir}); code != 0 {
		t.Fatalf("owner extract exit = %d", code)
	}
	info, err := os.Stat(filepath.Join(dir, "f"))
	if err != nil {
		t.Fatal(err)
	}
	if stat := info.Sys().(*syscall.Stat_t); int(stat.Uid) != uid || int(stat.Gid) != gid {
		t.Fatalf("owner = %d:%d", stat.Uid, stat.Gid)
	}
	h.Stdin = archive(t)
	if code := h.Main([]string{"tar-extract", "--owner-user", "nobody-here", dir}); code != exitFailure {
		t.Fatalf("unknown owner exit = %d", code)
	}
}

func TestTarExtractHeaderOwnershipAsRoot(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("chown to another user requires root")
	}
	dir := t.TempDir()
	if code, stderr := runExtract(t, archive(t, entry{header: tar.Header{Name: "f", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 4321, Gid: 8765}}), dir); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr)
	}
	info, _ := os.Stat(filepath.Join(dir, "f"))
	if stat := info.Sys().(*syscall.Stat_t); stat.Uid != 4321 || stat.Gid != 8765 {
		t.Fatalf("owner = %d:%d", stat.Uid, stat.Gid)
	}
}

func TestResolveOwner(t *testing.T) {
	passwd := []byte("root:x:0:0::/root:/bin/sh\n# comment\napp:x:1000:1001::/home/app:/bin/sh\nbad:x:nan:1\n")
	group := []byte("root:x:0:\nstaff:x:50:app\n")
	cases := []struct {
		spec     string
		uid, gid int
		fails    bool
	}{
		{"", 0, 0, false},
		{"app", 1000, 1001, false},
		{"1000", 1000, 1001, false},
		{"2000", 2000, 0, false},
		{"app:staff", 1000, 50, false},
		{"app:7", 1000, 7, false},
		{"2000:staff", 2000, 50, false},
		{"3:4", 3, 4, false},
		{"missing", 0, 0, true},
		{"app:missing", 0, 0, true},
		{"bad", 0, 0, true},
		{":1", 0, 0, true},
		{"app:", 0, 0, true},
	}
	for _, tc := range cases {
		uid, gid, err := ResolveOwner(tc.spec, passwd, group)
		if tc.fails {
			if err == nil {
				t.Errorf("%q resolved to %d:%d", tc.spec, uid, gid)
			}
			continue
		}
		if err != nil || uid != tc.uid || gid != tc.gid {
			t.Errorf("%q = %d:%d, %v; want %d:%d", tc.spec, uid, gid, err, tc.uid, tc.gid)
		}
	}
}

func TestTarCreateMissing(t *testing.T) {
	var stdout bytes.Buffer
	h := &Helper{Stdout: &stdout, Stderr: io.Discard}
	if code := h.Main([]string{"tar-create", filepath.Join(t.TempDir(), "missing")}); code != ExitNotFound || stdout.Len() != 0 {
		t.Fatalf("missing exit = %d, output %q", code, stdout.String())
	}
}

func TestTarCreateRoundTrip(t *testing.T) {
	source := filepath.Join(t.TempDir(), "tree")
	mtime := time.Date(2021, 5, 6, 7, 8, 9, 0, time.UTC)
	if err := os.MkdirAll(filepath.Join(source, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "sub/file"), []byte("payload"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(filepath.Join(source, "sub/file"), mtime, mtime); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("sub/file", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	var stdout bytes.Buffer
	h := &Helper{Stdout: &stdout, Stderr: io.Discard}
	if code := h.Main([]string{"tar-create", source}); code != 0 {
		t.Fatalf("create exit = %d", code)
	}
	reader := bufio.NewReader(&stdout)
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var stat api.FileStat
	if err := json.Unmarshal(line, &stat); err != nil {
		t.Fatal(err)
	}
	if stat.Name != "tree" || !stat.Mode.IsDir() {
		t.Fatalf("stat = %+v", stat)
	}
	destination := t.TempDir()
	if code, stderr := runExtract(t, reader, destination); code != 0 {
		t.Fatalf("extract exit %d: %s", code, stderr)
	}
	info, err := os.Stat(filepath.Join(destination, "tree/sub/file"))
	if err != nil || info.Mode() != 0o640 || !info.ModTime().Equal(mtime) {
		t.Fatalf("round-trip file = %v, %v", info, err)
	}
	if target, err := os.Readlink(filepath.Join(destination, "tree/link")); err != nil || target != "sub/file" {
		t.Fatalf("round-trip link = %q, %v", target, err)
	}

	var single bytes.Buffer
	h.Stdout = &single
	if code := h.Main([]string{"tar-create", filepath.Join(source, "sub/file")}); code != 0 {
		t.Fatalf("single create exit = %d", code)
	}
	line, _ = bufio.NewReader(bytes.NewReader(single.Bytes())).ReadBytes('\n')
	if err := json.Unmarshal(line, &stat); err != nil || stat.Name != "file" || stat.Size != 7 || stat.Mode != 0o640 {
		t.Fatalf("single stat = %+v, %v", stat, err)
	}
}
