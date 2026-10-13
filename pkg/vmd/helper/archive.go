package helper

import (
	"archive/tar"
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

// tarExtract unpacks stdin beneath DIR, mirroring Docker's CopyToContainer
// with CopyUIDGID when --owner-user is given.
func (h *Helper) tarExtract(args []string) int {
	flags := flag.NewFlagSet("tar-extract", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	ownerSpec := flags.String("owner-user", "", "image USER that owns extracted entries")
	if err := flags.Parse(args); err != nil {
		return h.usage("tar-extract: " + err.Error())
	}
	if flags.NArg() != 1 || !filepath.IsAbs(flags.Arg(0)) {
		return h.usage("tar-extract requires one absolute directory")
	}
	ownerSet := false
	flags.Visit(func(f *flag.Flag) { ownerSet = ownerSet || f.Name == "owner-user" })
	dir := filepath.Clean(flags.Arg(0))
	info, err := os.Stat(dir)
	if errors.Is(err, fs.ErrNotExist) || err == nil && !info.IsDir() {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: destination %s is not an existing directory\n", dir)
		return ExitNotFound
	}
	if err != nil {
		return h.fail(err)
	}
	var owner *ownerIDs
	if ownerSet {
		passwd, err := readOptional(filepath.Join(h.Root, "etc/passwd"))
		if err != nil {
			return h.fail(err)
		}
		group, err := readOptional(filepath.Join(h.Root, "etc/group"))
		if err != nil {
			return h.fail(err)
		}
		uid, gid, err := ResolveOwner(*ownerSpec, passwd, group)
		if err != nil {
			return h.fail(err)
		}
		owner = &ownerIDs{uid: uid, gid: gid}
	}
	if err := extract(dir, tar.NewReader(h.Stdin), owner); err != nil {
		return h.fail(err)
	}
	return 0
}

type ownerIDs struct{ uid, gid int }

type dirTimes struct {
	path  string
	mtime time.Time
}

func extract(dir string, reader *tar.Reader, owner *ownerIDs) error {
	var directories []dirTimes
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read archive: %w", err)
		}
		relative, err := entryName(header.Name)
		if err != nil {
			return err
		}
		if relative == "." {
			continue
		}
		target := filepath.Join(dir, relative)
		if err := prepareParents(dir, relative); err != nil {
			return err
		}
		if err := extractEntry(dir, target, header, reader); err != nil {
			return fmt.Errorf("extract %s: %w", header.Name, err)
		}
		// A hard link shares its target's inode and metadata.
		if header.Typeflag == tar.TypeLink {
			continue
		}
		uid, gid := header.Uid, header.Gid
		if owner != nil {
			uid, gid = owner.uid, owner.gid
		}
		if err := os.Lchown(target, uid, gid); err != nil {
			return fmt.Errorf("chown %s: %w", header.Name, err)
		}
		// Ownership changes clear setuid bits, so modes follow chown.
		if header.Typeflag != tar.TypeSymlink {
			if err := unix.Chmod(target, uint32(header.Mode&0o7777)); err != nil {
				return fmt.Errorf("chmod %s: %w", header.Name, err)
			}
		}
		if header.Typeflag == tar.TypeDir {
			directories = append(directories, dirTimes{path: target, mtime: header.ModTime})
			continue
		}
		if err := setTimes(target, header.ModTime); err != nil {
			return fmt.Errorf("set times %s: %w", header.Name, err)
		}
	}
	// Children change directory mtimes, so directories are stamped last.
	for i := len(directories) - 1; i >= 0; i-- {
		if err := setTimes(directories[i].path, directories[i].mtime); err != nil {
			return fmt.Errorf("set directory times: %w", err)
		}
	}
	return nil
}

// entryName validates an archive name and returns it relative to the
// destination; absolute names and parent traversal are rejected.
func entryName(name string) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", fmt.Errorf("archive entry %q is not a relative path", name)
	}
	clean := filepath.Clean(name)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("archive entry %q escapes the destination", name)
	}
	return clean, nil
}

// prepareParents creates missing intermediate directories and refuses any
// existing component that is not a real directory, so no symlink is followed.
func prepareParents(dir, relative string) error {
	current := dir
	parts := strings.Split(relative, "/")
	for _, part := range parts[:len(parts)-1] {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if errors.Is(err, fs.ErrNotExist) {
			if err := os.Mkdir(current, 0o755); err != nil {
				return fmt.Errorf("create parent %s: %w", current, err)
			}
			continue
		}
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("archive path component %s is not a directory", current)
		}
	}
	return nil
}

func extractEntry(dir, target string, header *tar.Header, content io.Reader) error {
	existing, err := os.Lstat(target)
	exists := err == nil
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if header.Typeflag == tar.TypeDir {
		if exists && existing.IsDir() {
			return nil
		}
		if exists {
			if err := os.Remove(target); err != nil {
				return err
			}
		}
		return os.Mkdir(target, 0o700)
	}
	if exists {
		// os.Remove refuses a non-empty directory, which must not be replaced.
		if err := os.Remove(target); err != nil {
			return fmt.Errorf("replace existing entry: %w", err)
		}
	}
	switch header.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		file, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0o600)
		if err != nil {
			return err
		}
		if _, err := io.Copy(file, content); err != nil {
			file.Close()
			return err
		}
		return file.Close()
	case tar.TypeSymlink:
		return os.Symlink(header.Linkname, target)
	case tar.TypeLink:
		relative, err := entryName(header.Linkname)
		if err != nil {
			return err
		}
		source := filepath.Join(dir, relative)
		if err := prepareParents(dir, relative); err != nil {
			return err
		}
		return os.Link(source, target)
	case tar.TypeFifo:
		return unix.Mkfifo(target, 0o600)
	case tar.TypeChar:
		return unix.Mknod(target, unix.S_IFCHR|0o600, int(unix.Mkdev(uint32(header.Devmajor), uint32(header.Devminor))))
	case tar.TypeBlock:
		return unix.Mknod(target, unix.S_IFBLK|0o600, int(unix.Mkdev(uint32(header.Devmajor), uint32(header.Devminor))))
	default:
		return fmt.Errorf("unsupported archive entry type %q", header.Typeflag)
	}
}

func setTimes(path string, mtime time.Time) error {
	stamp := unix.NsecToTimespec(mtime.UnixNano())
	return unix.UtimesNanoAt(unix.AT_FDCWD, path, []unix.Timespec{stamp, stamp}, unix.AT_SYMLINK_NOFOLLOW)
}

func readOptional(path string) ([]byte, error) {
	content, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	return content, err
}

// ResolveOwner resolves an image USER spec ("", "user", or "user:group",
// where each part is a name or a numeric ID) against passwd and group file
// contents. An empty spec is root. A numeric user absent from passwd uses
// group 0, as Docker does.
func ResolveOwner(spec string, passwd, group []byte) (int, int, error) {
	if spec == "" {
		return 0, 0, nil
	}
	userPart, groupPart, hasGroup := strings.Cut(spec, ":")
	if userPart == "" || hasGroup && groupPart == "" {
		return 0, 0, fmt.Errorf("invalid user spec %q", spec)
	}
	uid, gid := -1, 0
	if id, err := strconv.Atoi(userPart); err == nil && id >= 0 {
		uid = id
		if entry, ok := findEntry(passwd, 2, userPart); ok {
			gid = entry.gid
		}
	} else {
		entry, ok := findEntry(passwd, 0, userPart)
		if !ok {
			return 0, 0, fmt.Errorf("user %q not found in /etc/passwd", userPart)
		}
		uid, gid = entry.id, entry.gid
	}
	if hasGroup {
		if id, err := strconv.Atoi(groupPart); err == nil && id >= 0 {
			gid = id
		} else {
			entry, ok := findEntry(group, 0, groupPart)
			if !ok {
				return 0, 0, fmt.Errorf("group %q not found in /etc/group", groupPart)
			}
			gid = entry.id
		}
	}
	return uid, gid, nil
}

type idEntry struct{ id, gid int }

// findEntry finds a passwd or group line whose field at index matches value.
// Field 2 is the numeric ID in both formats; passwd field 3 is the primary group.
func findEntry(content []byte, index int, value string) (idEntry, bool) {
	scanner := bufio.NewScanner(strings.NewReader(string(content)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, ":")
		if len(fields) < 3 || fields[index] != value {
			continue
		}
		id, err := strconv.Atoi(fields[2])
		if err != nil {
			continue
		}
		entry := idEntry{id: id}
		if len(fields) > 3 {
			if gid, err := strconv.Atoi(fields[3]); err == nil {
				entry.gid = gid
			}
		}
		return entry, true
	}
	return idEntry{}, false
}

// tarCreate writes one JSON stat line and then a tar stream of PATH.
func (h *Helper) tarCreate(args []string) int {
	if len(args) != 1 || !filepath.IsAbs(args[0]) {
		return h.usage("tar-create requires one absolute path")
	}
	root := filepath.Clean(args[0])
	info, err := os.Lstat(root)
	if errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(h.Stderr, "aries-vmd-helper: %s not found\n", root)
		return ExitNotFound
	}
	if err != nil {
		return h.fail(err)
	}
	output := bufio.NewWriterSize(h.Stdout, 64<<10)
	line, err := json.Marshal(FileStat{Name: filepath.Base(root), Size: info.Size(), Mode: info.Mode()})
	if err != nil {
		return h.fail(err)
	}
	if _, err := output.Write(append(line, '\n')); err != nil {
		return h.fail(err)
	}
	writer := tar.NewWriter(output)
	base := filepath.Base(root)
	walkErr := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		return writeEntry(writer, path, filepath.Join(base, relative))
	})
	if err := errors.Join(walkErr, writer.Close(), output.Flush()); err != nil {
		return h.fail(err)
	}
	return 0
}

func writeEntry(writer *tar.Writer, path, name string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	link := ""
	if info.Mode()&fs.ModeSymlink != 0 {
		if link, err = os.Readlink(path); err != nil {
			return err
		}
	}
	header, err := tar.FileInfoHeader(info, link)
	if err != nil {
		return err
	}
	header.Name = name
	if info.IsDir() {
		header.Name += "/"
	}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	// A file that grows while archived must not overrun its header size.
	_, err = io.Copy(writer, io.LimitReader(file, header.Size))
	return err
}
