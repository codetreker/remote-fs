package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const defaultCredentialFile = ".remote-fs/credentials.json"

type pendingRotation struct {
	Secret             string `json:"secret"`
	ActionID           string `json:"actionId"`
	ExpectedGeneration string `json:"expectedGeneration"`
}
type credentialRecord struct {
	Version    int              `json:"version"`
	Server     string           `json:"server"`
	ID         string           `json:"id"`
	Secret     string           `json:"secret"`
	SubjectID  string           `json:"subjectId"`
	Generation string           `json:"generation"`
	Pending    *pendingRotation `json:"pending,omitempty"`
}

func privateDirectory(name string, create bool) (*os.File, string, error) {
	absolute, err := filepath.Abs(name)
	if err != nil {
		return nil, "", err
	}
	parent := filepath.Dir(absolute)
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, "", err
	}
	for _, part := range strings.Split(strings.TrimPrefix(parent, "/"), "/") {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(openErr, unix.ENOENT) && create {
			mkdirErr := unix.Mkdirat(fd, part, 0700)
			if mkdirErr != nil && !errors.Is(mkdirErr, unix.EEXIST) {
				unix.Close(fd)
				return nil, "", mkdirErr
			}
			next, openErr = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
			if openErr == nil {
				openErr = unix.Fsync(fd)
			}
		}
		unix.Close(fd)
		if openErr != nil {
			if next >= 0 {
				unix.Close(next)
			}
			return nil, "", openErr
		}
		fd = next
		var st unix.Stat_t
		if err = unix.Fstat(fd, &st); err != nil {
			unix.Close(fd)
			return nil, "", err
		}
		if st.Mode&0022 != 0 && !(st.Mode&unix.S_ISVTX != 0 && st.Uid == 0) {
			unix.Close(fd)
			return nil, "", errors.New("private file ancestor is writable by another user")
		}
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		unix.Close(fd)
		return nil, "", err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&0777 != 0700 {
		unix.Close(fd)
		return nil, "", errors.New("private file directory must be owned by this user with mode 0700")
	}
	return os.NewFile(uintptr(fd), parent), filepath.Base(absolute), nil
}
func checkPrivateStat(st unix.Stat_t) error {
	if st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0777 != 0600 || st.Uid != uint32(os.Geteuid()) {
		return errors.New("private file must be a regular file owned by this user with mode 0600")
	}
	return nil
}
func readPrivate(name string, result any) error {
	dir, leaf, err := privateDirectory(name, false)
	if err != nil {
		return err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), leaf, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), leaf)
	defer file.Close()
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err != nil {
		return err
	}
	if err = checkPrivateStat(st); err != nil {
		return err
	}
	if st.Size > 64<<10 {
		return errors.New("private file exceeds its size limit")
	}
	decoder := json.NewDecoder(io.LimitReader(file, (64<<10)+1))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(result); err != nil {
		return errors.New("invalid private file")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("private file contains trailing data")
	}
	return nil
}
func writePrivate(name string, record any) error {
	dir, leaf, err := privateDirectory(name, true)
	if err != nil {
		return err
	}
	defer dir.Close()
	var st unix.Stat_t
	if err = unix.Fstatat(int(dir.Fd()), leaf, &st, unix.AT_SYMLINK_NOFOLLOW); err == nil {
		if err = checkPrivateStat(st); err != nil {
			return err
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return err
	}
	encoded, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if len(encoded) > 64<<10 {
		return errors.New("private record exceeds its size limit")
	}
	nonce, err := randomHex()
	if err != nil {
		return err
	}
	tempName := ".credentials-" + nonce
	fd, err := unix.Openat(int(dir.Fd()), tempName, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return err
	}
	defer unix.Unlinkat(int(dir.Fd()), tempName, 0)
	temp := os.NewFile(uintptr(fd), tempName)
	if _, err = temp.Write(append(encoded, '\n')); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Sync(); err != nil {
		return errors.Join(err, temp.Close())
	}
	if err = temp.Close(); err != nil {
		return err
	}
	if err = unix.Renameat(int(dir.Fd()), tempName, int(dir.Fd()), leaf); err != nil {
		return err
	}
	return dir.Sync()
}
func lockPrivate(name string) (func() error, error) {
	dir, leaf, err := privateDirectory(name, true)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	fd, err := unix.Openat(int(dir.Fd()), leaf+".lock", unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if err = unix.Fstat(fd, &st); err == nil {
		err = checkPrivateStat(st)
	}
	if err == nil {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
	}
	if err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("private file is busy or unsafe: %w", err)
	}
	return func() error { return errors.Join(unix.Flock(fd, unix.LOCK_UN), unix.Close(fd)) }, nil
}

func loadCredential(name string) (credentialRecord, error) {
	var record credentialRecord
	if err := readPrivate(name, &record); err != nil {
		return record, err
	}
	if record.Version != 1 || len(record.ID) != 32 || len(record.SubjectID) != 32 || record.Generation == "" {
		return record, errors.New("invalid credential record")
	}
	if _, err := hex.DecodeString(record.ID); err != nil {
		return record, errors.New("invalid credential identifier")
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(record.Secret)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != record.Secret {
		return record, errors.New("invalid credential secret")
	}
	return record, nil
}

func randomHex() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func randomSecret() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b[:]), nil
}

func validateServiceURL(raw string, insecure bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" && u.Path != "/" {
		return "", errors.New("-server must be a service origin without credentials, path, query or fragment")
	}
	if u.Scheme != "https" && !(insecure && u.Scheme == "http") {
		return "", errors.New("service credentials require HTTPS; -allow-insecure-http permits an explicit development connection")
	}
	return strings.TrimSuffix(u.String(), "/"), nil
}

type fileCredentialTransport struct {
	path, origin string
	base         http.RoundTripper
}

func (t fileCredentialTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c, err := loadCredential(t.path)
	if err != nil {
		return nil, errors.New("cannot load private credential file")
	}
	u, err := url.Parse(c.Server)
	if err != nil {
		return nil, errors.New("invalid credential server")
	}
	if r.URL.Scheme != u.Scheme || r.URL.Host != u.Host || r.URL.User != nil || r.URL.Opaque != "" || r.Host != "" && r.Host != u.Host || c.Server != t.origin {
		return nil, errors.New("credential server does not match the request origin")
	}
	copy := r.Clone(r.Context())
	copy.Header = r.Header.Clone()
	copy.Header.Del("Cookie")
	copy.Header.Set("Authorization", "Bearer "+c.ID+"."+c.Secret)
	return t.base.RoundTrip(copy)
}
func credentialHTTPClient(file, server string, timeout time.Duration) (*http.Client, error) {
	c, err := loadCredential(file)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(server)
	if err != nil || u.User != nil || u.Scheme+"://"+u.Host != c.Server {
		return nil, errors.New("mount URL does not match credential server")
	}
	client := callerClient(timeout)
	client.Transport = fileCredentialTransport{file, c.Server, client.Transport}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("authenticated redirects are refused") }
	return client, nil
}
