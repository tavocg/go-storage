// Package fs implements a filesystem-backed storage.Backend.
package fs

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	storage "github.com/tavocg/go-storage"
)

const defaultMaxSize = int64(10 * 1024 * 1024 * 1024)

type Options struct {
	Root    string
	MaxSize int64
}

type backend struct {
	root string
}

func WithRoot(root string) func(*Options) {
	return func(o *Options) {
		o.Root = root
	}
}

func WithMaxSize(maxSize int64) func(*Options) {
	return func(o *Options) {
		o.MaxSize = maxSize
	}
}

func New(ctx context.Context, optFuncs ...func(*Options)) (*storage.Storage, error) {
	opts := Options{
		MaxSize: defaultMaxSize,
	}
	for _, optFunc := range optFuncs {
		optFunc(&opts)
	}

	root := strings.TrimSpace(opts.Root)
	if root == "" {
		return nil, fmt.Errorf("fs root is required")
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root: %w", err)
	}

	if err := os.MkdirAll(absRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create root: %w", err)
	}
	if err := ensureDir(absRoot); err != nil {
		return nil, fmt.Errorf("validate root: %w", err)
	}

	store := storage.New(&backend{root: absRoot}, "", opts.MaxSize)
	if err := store.LoadState(ctx); err != nil {
		return nil, fmt.Errorf("load storage state: %w", err)
	}

	return store, nil
}

func (b *backend) PutObject(ctx context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	targetPath, err := b.objectPath(aws.ToString(params.Key), true)
	if err != nil {
		return nil, err
	}

	tmpFile, err := os.CreateTemp(filepath.Dir(targetPath), ".store-*")
	if err != nil {
		return nil, fmt.Errorf("create temp file: %w", err)
	}

	tmpPath := tmpFile.Name()
	defer func() {
		_ = tmpFile.Close()
	}()

	if _, err := io.Copy(tmpFile, params.Body); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("write object: %w", err)
	}

	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("close temp file: %w", err)
	}

	select {
	case <-ctx.Done():
		_ = os.Remove(tmpPath)
		return nil, ctx.Err()
	default:
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return nil, fmt.Errorf("move object into place: %w", err)
	}

	return &s3.PutObjectOutput{}, nil
}

func (b *backend) GetObject(ctx context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	targetPath, err := b.objectPath(aws.ToString(params.Key), false)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("open object: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat object: %w", err)
	}

	select {
	case <-ctx.Done():
		_ = file.Close()
		return nil, ctx.Err()
	default:
	}

	return &s3.GetObjectOutput{
		Body:          file,
		ContentLength: aws.Int64(info.Size()),
	}, nil
}

func (b *backend) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	targetPath, err := b.objectPath(aws.ToString(params.Key), false)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := os.Remove(targetPath); err != nil {
		if os.IsNotExist(err) {
			return nil, storage.ErrObjectNotFound
		}
		return nil, fmt.Errorf("delete object: %w", err)
	}

	b.removeEmptyParents(filepath.Dir(targetPath))

	return &s3.DeleteObjectOutput{}, nil
}

func (b *backend) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	objects, err := b.listObjects(ctx, aws.ToString(params.Prefix))
	if err != nil {
		return nil, err
	}

	start := 0
	if token := aws.ToString(params.ContinuationToken); token != "" {
		for i, obj := range objects {
			if aws.ToString(obj.Key) == token {
				start = i + 1
				break
			}
		}
	}
	if start == 0 {
		if after := aws.ToString(params.StartAfter); after != "" {
			for i, obj := range objects {
				if aws.ToString(obj.Key) > after {
					start = i
					break
				}
			}
			if len(objects) > 0 && aws.ToString(objects[len(objects)-1].Key) <= after {
				start = len(objects)
			}
		}
	}

	end := len(objects)
	if params.MaxKeys != nil && *params.MaxKeys > 0 {
		maxKeys := int(*params.MaxKeys)
		if start+maxKeys < end {
			end = start + maxKeys
		}
	}

	page := objects[start:end]
	out := &s3.ListObjectsV2Output{
		Contents: page,
	}
	if end < len(objects) {
		out.IsTruncated = aws.Bool(true)
		out.NextContinuationToken = page[len(page)-1].Key
	} else {
		out.IsTruncated = aws.Bool(false)
	}

	return out, nil
}

func (b *backend) listObjects(ctx context.Context, prefix string) ([]types.Object, error) {
	objects := make([]types.Object, 0)

	err := filepath.WalkDir(b.root, func(currentPath string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink entry %q is not allowed", currentPath)
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(b.root, currentPath)
		if err != nil {
			return err
		}

		key := filepath.ToSlash(relPath)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}

		objects = append(objects, types.Object{
			Key:  aws.String(key),
			Size: aws.Int64(info.Size()),
		})

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	sort.Slice(objects, func(i, j int) bool {
		return aws.ToString(objects[i].Key) < aws.ToString(objects[j].Key)
	})

	return objects, nil
}

func (b *backend) objectPath(key string, createParents bool) (string, error) {
	cleanKey, err := normalizeKey(key)
	if err != nil {
		return "", err
	}

	dir := filepath.Dir(filepath.FromSlash(cleanKey))
	if dir == "." {
		dir = ""
	}
	if err := b.ensureDirChain(dir, createParents); err != nil {
		return "", err
	}

	targetPath := filepath.Join(b.root, filepath.FromSlash(cleanKey))
	if err := ensureNoSymlink(targetPath); err != nil && !os.IsNotExist(err) {
		return "", err
	}

	return targetPath, nil
}

func normalizeKey(key string) (string, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return "", fmt.Errorf("object key is required")
	}

	cleanKey := strings.TrimPrefix(path.Clean("/"+key), "/")
	if cleanKey == "" || cleanKey == "." {
		return "", fmt.Errorf("object key is required")
	}

	return cleanKey, nil
}

func (b *backend) removeEmptyParents(dir string) {
	for dir != b.root && dir != "." && dir != string(filepath.Separator) {
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}

func (b *backend) ensureDirChain(dir string, create bool) error {
	current := b.root
	if err := ensureDir(current); err != nil {
		return err
	}
	if dir == "" {
		return nil
	}

	for _, part := range strings.Split(dir, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}

		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("inspect path %q: %w", current, err)
			}
			if !create {
				return err
			}

			if err := os.Mkdir(current, 0o755); err != nil && !os.IsExist(err) {
				return fmt.Errorf("create directory %q: %w", current, err)
			}

			info, err = os.Lstat(current)
			if err != nil {
				return fmt.Errorf("inspect created path %q: %w", current, err)
			}
		}

		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink path component %q is not allowed", current)
		}
		if !info.IsDir() {
			return fmt.Errorf("path component %q is not a directory", current)
		}
	}

	return nil
}

func ensureDir(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlink path component %q is not allowed", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("path component %q is not a directory", path)
	}

	return nil
}

func ensureNoSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("symlink path component %q is not allowed", path)
	}

	return nil
}
