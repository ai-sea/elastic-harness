package objectstorelocal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const referencePrefix = "artifact://sha256/"

// Store 以内容摘要作为不可变对象键，重复写入会收敛到同一 ArtifactRef。
type Store struct {
	root string
}

func New(root string) (*Store, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absolute, 0o700); err != nil {
		return nil, err
	}
	return &Store{root: absolute}, nil
}

func (s *Store) Put(ctx context.Context, _ string, source io.Reader) (string, error) {
	temporary, err := os.CreateTemp(s.root, "artifact-*.tmp")
	if err != nil {
		return "", err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	hash := sha256.New()
	if _, err := io.Copy(io.MultiWriter(temporary, hash), &contextReader{ctx: ctx, reader: source}); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return "", err
	}
	if err := temporary.Close(); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	target := filepath.Join(s.root, digest)
	if _, err := os.Stat(target); err == nil {
		return referencePrefix + digest, nil
	}
	if err := os.Rename(temporaryName, target); err != nil {
		return "", err
	}
	return referencePrefix + digest, nil
}

func (s *Store) Get(_ context.Context, reference string) (io.ReadCloser, error) {
	if !strings.HasPrefix(reference, referencePrefix) {
		return nil, errors.New("ArtifactRef 格式无效")
	}
	digest := strings.TrimPrefix(reference, referencePrefix)
	if len(digest) != sha256.Size*2 {
		return nil, errors.New("ArtifactRef 摘要长度无效")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return nil, fmt.Errorf("ArtifactRef 摘要无效：%w", err)
	}
	return os.Open(filepath.Join(s.root, digest))
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
		return r.reader.Read(buffer)
	}
}
