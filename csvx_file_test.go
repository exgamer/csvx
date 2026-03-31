package csvx

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestParseFileToSlice(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "users.csv")

	data := `name,age,city,active
Alice,25,New York,true
Bob,30,Los Angeles,false
`

	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	got, err := ParseFileToSlice(
		context.Background(),
		path,
		testUserMapper,
		ParseOptions{},
	)
	if err != nil {
		t.Fatalf("ParseFileToSlice error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 users, got %d", len(got))
	}
}
