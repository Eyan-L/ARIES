package server

import (
	"fmt"
	"strconv"
	"strings"
)

// identity is a resolved numeric execution identity inside a sandbox.
type identity struct {
	UID            uint32   `json:"uid"`
	GID            uint32   `json:"gid"`
	AdditionalGIDs []uint32 `json:"additional_gids,omitempty"`
}

type passwdEntry struct {
	name string
	uid  uint32
	gid  uint32
}

type groupEntry struct {
	name    string
	gid     uint32
	members []string
}

func parsePasswd(content string) []passwdEntry {
	var entries []passwdEntry
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Split(strings.TrimSpace(line), ":")
		if len(fields) < 4 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		uid, uidErr := strconv.ParseUint(fields[2], 10, 32)
		gid, gidErr := strconv.ParseUint(fields[3], 10, 32)
		if uidErr != nil || gidErr != nil {
			continue
		}
		entries = append(entries, passwdEntry{name: fields[0], uid: uint32(uid), gid: uint32(gid)})
	}
	return entries
}

func parseGroup(content string) []groupEntry {
	var entries []groupEntry
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Split(strings.TrimSpace(line), ":")
		if len(fields) < 3 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		gid, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil {
			continue
		}
		var members []string
		if len(fields) > 3 && fields[3] != "" {
			members = strings.Split(fields[3], ",")
		}
		entries = append(entries, groupEntry{name: fields[0], gid: uint32(gid), members: members})
	}
	return entries
}

// resolveUser follows Docker's user semantics against the sandbox's own
// /etc/passwd and /etc/group, captured before boot: "" is root, "uid" or
// "name" takes the primary group from passwd (else 0), and "user:group"
// resolves each half. Supplementary groups come from group membership.
func resolveUser(user, passwd, group string) (identity, error) {
	users := parsePasswd(passwd)
	groups := parseGroup(group)
	if user == "" {
		user = "0"
	}
	userPart, groupPart, hasGroup := strings.Cut(user, ":")
	if userPart == "" || hasGroup && groupPart == "" {
		return identity{}, fmt.Errorf("invalid user %q", user)
	}
	var result identity
	var name string
	if value, err := strconv.ParseUint(userPart, 10, 32); err == nil {
		result.UID = uint32(value)
		for _, entry := range users {
			if entry.uid == result.UID {
				name, result.GID = entry.name, entry.gid
				break
			}
		}
	} else {
		found := false
		for _, entry := range users {
			if entry.name == userPart {
				name, result.UID, result.GID, found = entry.name, entry.uid, entry.gid, true
				break
			}
		}
		if !found {
			return identity{}, fmt.Errorf("user %q is not in the sandbox passwd file", userPart)
		}
	}
	if hasGroup {
		if value, err := strconv.ParseUint(groupPart, 10, 32); err == nil {
			result.GID = uint32(value)
		} else {
			found := false
			for _, entry := range groups {
				if entry.name == groupPart {
					result.GID, found = entry.gid, true
					break
				}
			}
			if !found {
				return identity{}, fmt.Errorf("group %q is not in the sandbox group file", groupPart)
			}
		}
	}
	if name != "" {
		for _, entry := range groups {
			if entry.gid == result.GID {
				continue
			}
			for _, member := range entry.members {
				if member == name {
					result.AdditionalGIDs = append(result.AdditionalGIDs, entry.gid)
					break
				}
			}
		}
	}
	return result, nil
}
