package fs_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	storage "github.com/tavocg/go-storage/v2"
	"github.com/tavocg/go-storage/v2/backends/fs"
	"github.com/tavocg/go-storage/v2/internal/storagetest"
)

func backend(t *testing.T) storage.Backend {
	t.Helper()
	b, err := fs.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := b.Close(); err != nil {
			t.Error(err)
		}
	})
	return b
}
func TestBackend(t *testing.T) { storagetest.Run(t, backend) }

func TestKeysCannotEscapeRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "objects")
	b, err := fs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	s := storagetest.New(t, b, storage.Options{})
	for _, key := range []string{"../outside", "/absolute", "a/../../outside", ".tmp-private", "a\\b"} {
		if _, err := s.Put(t.Context(), key, strings.NewReader("safe")); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(parent, "outside")); !os.IsNotExist(err) {
		t.Fatalf("escaped root: %v", err)
	}
}

func TestIgnoreStagingAndRejectSymlinks(t *testing.T) {
	root := t.TempDir()
	b, err := fs.New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := os.WriteFile(filepath.Join(root, ".tmp-interrupted"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	objects, err := b.List(t.Context())
	if err != nil || len(objects) != 0 {
		t.Fatalf("temporary file listed: %v %v", objects, err)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "obj-symlink")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.List(t.Context()); err == nil {
		t.Fatal("symlink accepted")
	}
}
