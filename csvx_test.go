package csvx

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

type testUser struct {
	Name   string
	Age    int
	City   string
	Active bool
}

func testUserMapper(row RowAccessor) (testUser, error) {
	name, err := row.Required("name")
	if err != nil {
		return testUser{}, err
	}

	age, err := row.Int("age")
	if err != nil {
		return testUser{}, err
	}

	active, err := row.Bool("active")
	if err != nil {
		return testUser{}, err
	}

	return testUser{
		Name:   name,
		Age:    age,
		City:   row.String("city"),
		Active: active,
	}, nil
}

func TestRowAccessor_Getters(t *testing.T) {
	t.Parallel()

	index := HeaderIndex{
		"name":   0,
		"age":    1,
		"city":   2,
		"active": 3,
	}

	row := NewRowAccessor(index, []string{" Alice ", "25", " New York ", "true"})

	if !row.HasColumn("name") {
		t.Fatalf("expected HasColumn(name)=true")
	}

	if row.HasColumn("missing") {
		t.Fatalf("expected HasColumn(missing)=false")
	}

	if got := row.Get("name"); got != " Alice " {
		t.Fatalf("unexpected Get(name): %q", got)
	}

	if got := row.String("name"); got != "Alice" {
		t.Fatalf("unexpected String(name): %q", got)
	}

	v, err := row.Required("name")
	if err != nil {
		t.Fatalf("Required(name) error: %v", err)
	}
	if v != "Alice" {
		t.Fatalf("unexpected Required(name): %q", v)
	}

	age, err := row.Int("age")
	if err != nil {
		t.Fatalf("Int(age) error: %v", err)
	}
	if age != 25 {
		t.Fatalf("unexpected Int(age): %d", age)
	}

	active, err := row.Bool("active")
	if err != nil {
		t.Fatalf("Bool(active) error: %v", err)
	}
	if !active {
		t.Fatalf("unexpected Bool(active): %v", active)
	}
}

func TestRowAccessor_RequiredErrors(t *testing.T) {
	t.Parallel()

	index := HeaderIndex{
		"name": 0,
		"age":  1,
	}

	row := NewRowAccessor(index, []string{"", "30"})

	if _, err := row.Required("missing"); err == nil {
		t.Fatal("expected error for missing field")
	}

	if _, err := row.Required("name"); err == nil {
		t.Fatal("expected error for empty field")
	}
}

func TestRowAccessor_ValuesCopy(t *testing.T) {
	t.Parallel()

	index := HeaderIndex{"name": 0}
	raw := []string{"Alice"}

	row := NewRowAccessor(index, raw)
	cp := row.ValuesCopy()

	if len(cp) != 1 || cp[0] != "Alice" {
		t.Fatalf("unexpected ValuesCopy: %#v", cp)
	}

	cp[0] = "Bob"

	if raw[0] != "Alice" {
		t.Fatalf("ValuesCopy should not affect original slice")
	}
}

func TestParseStream_Success(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30,Los Angeles,false
`

	var got []testUser

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			got = append(got, item)
			return nil
		},
		ParseOptions{},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 users, got %d", len(got))
	}

	if got[0].Name != "Alice" || got[0].Age != 25 || !got[0].Active {
		t.Fatalf("unexpected first user: %+v", got[0])
	}

	if got[1].Name != "Bob" || got[1].Age != 30 || got[1].Active {
		t.Fatalf("unexpected second user: %+v", got[1])
	}
}

func TestParseStream_EmptyCSV(t *testing.T) {
	t.Parallel()

	err := ParseStream(
		context.Background(),
		strings.NewReader(""),
		testUserMapper,
		func(item testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected error for empty csv")
	}
}

func TestParseStream_HeaderValidation_Duplicate(t *testing.T) {
	t.Parallel()

	csvData := `name,age,name
Alice,25,Bob
`

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected duplicate header error")
	}
}

func TestParseStream_AllowShortRows(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30
`

	var got []testUser

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		func(row RowAccessor) (testUser, error) {
			name, err := row.Required("name")
			if err != nil {
				return testUser{}, err
			}

			age, err := row.Int("age")
			if err != nil {
				return testUser{}, err
			}

			return testUser{
				Name:   name,
				Age:    age,
				City:   row.String("city"),
				Active: false,
			}, nil
		},
		func(item testUser) error {
			got = append(got, item)
			return nil
		},
		ParseOptions{
			AllowShortRows: true,
		},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}

	if got[1].City != "" {
		t.Fatalf("expected empty city for short row, got %q", got[1].City)
	}
}

func TestParseStream_DisallowExtraColumns(t *testing.T) {
	t.Parallel()

	csvData := `name,age
Alice,25,extra
`

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		func(row RowAccessor) (testUser, error) {
			return testUser{}, nil
		},
		func(item testUser) error { return nil },
		ParseOptions{
			DisallowExtraColumns: true,
		},
	)
	if err == nil {
		t.Fatal("expected extra columns error")
	}

	var rowErr *RowError
	if !errors.As(err, &rowErr) {
		t.Fatalf("expected RowError, got %T", err)
	}

	if rowErr.Line != 2 {
		t.Fatalf("expected line 2, got %d", rowErr.Line)
	}
}

func TestParseStream_SkipDecodeErrors(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,not-int,LA,false
Charlie,35,Chicago,true
`

	var got []testUser
	var rowErrCount int32

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			got = append(got, item)
			return nil
		},
		ParseOptions{
			SkipDecodeErrors: true,
			OnRowError: func(err *RowError) {
				atomic.AddInt32(&rowErrCount, 1)
			},
		},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 successful rows, got %d", len(got))
	}

	if rowErrCount != 1 {
		t.Fatalf("expected 1 row error, got %d", rowErrCount)
	}

	if got[0].Name != "Alice" || got[1].Name != "Charlie" {
		t.Fatalf("unexpected parsed users: %+v", got)
	}
}

func TestParseStream_HandlerError_NoSkip(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30,LA,false
`

	handlerErr := errors.New("handler failed")

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			if item.Name == "Bob" {
				return handlerErr
			}
			return nil
		},
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected handler error")
	}

	var rowErr *RowError
	if !errors.As(err, &rowErr) {
		t.Fatalf("expected RowError, got %T", err)
	}

	if rowErr.Line != 3 {
		t.Fatalf("expected line 3, got %d", rowErr.Line)
	}
}

func TestParseStream_HandlerError_Skip(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30,LA,false
Charlie,35,Chicago,true
`

	var got []testUser
	var rowErrCount int32

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			if item.Name == "Bob" {
				return errors.New("handler failed")
			}
			got = append(got, item)
			return nil
		},
		ParseOptions{
			SkipHandlerErrors: true,
			OnRowError: func(err *RowError) {
				atomic.AddInt32(&rowErrCount, 1)
			},
		},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 successful handler rows, got %d", len(got))
	}

	if rowErrCount != 1 {
		t.Fatalf("expected 1 handler row error, got %d", rowErrCount)
	}
}

func TestParseStream_MaxRowErrors(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,bad,New York,true
Bob,bad,LA,false
Charlie,35,Chicago,true
`

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error { return nil },
		ParseOptions{
			SkipDecodeErrors: true,
			MaxRowErrors:     1,
		},
	)
	if err == nil {
		t.Fatal("expected max row errors error")
	}
}

func TestParseInBatches(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30,Los Angeles,false
Charlie,35,Chicago,true
Dave,40,Houston,false
Eve,45,Boston,true
`

	var batchSizes []int
	total := 0

	err := ParseInBatches(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		2,
		func(batch []testUser) error {
			batchSizes = append(batchSizes, len(batch))
			total += len(batch)
			return nil
		},
		ParseOptions{},
	)
	if err != nil {
		t.Fatalf("ParseInBatches error: %v", err)
	}

	if total != 5 {
		t.Fatalf("expected total=5, got %d", total)
	}

	if len(batchSizes) != 3 {
		t.Fatalf("expected 3 batches, got %d", len(batchSizes))
	}

	expected := []int{2, 2, 1}
	for i := range expected {
		if batchSizes[i] != expected[i] {
			t.Fatalf("unexpected batch size at %d: got=%d want=%d", i, batchSizes[i], expected[i])
		}
	}
}

func TestParseInBatches_InvalidBatchSize(t *testing.T) {
	t.Parallel()

	err := ParseInBatches(
		context.Background(),
		strings.NewReader("name,age\nAlice,25\n"),
		testUserMapper,
		0,
		func(batch []testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected invalid batch size error")
	}
}

func TestParseStreamToChannel(t *testing.T) {
	t.Parallel()

	csvData := `name,age,city,active
Alice,25,New York,true
Bob,30,Los Angeles,false
`

	items, errs := ParseStreamToChannel(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		ParseOptions{},
		2,
	)

	var got []testUser
	for item := range items {
		got = append(got, item)
	}

	if err := <-errs; err != nil {
		t.Fatalf("unexpected channel parse error: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 items, got %d", len(got))
	}
}

func TestParseStream_ContextCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := ParseStream(
		ctx,
		strings.NewReader("name,age,city,active\nAlice,25,NY,true\n"),
		testUserMapper,
		func(item testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected context error")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestParseStream_TrimHeaderSpaceAndBOM(t *testing.T) {
	t.Parallel()

	csvData := "\uFEFF name , age , city , active \nAlice,25,New York,true\n"

	var got []testUser

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			got = append(got, item)
			return nil
		},
		ParseOptions{
			TrimHeaderSpace: true,
			TrimUTF8BOM:     true,
		},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}

	if got[0].Name != "Alice" {
		t.Fatalf("unexpected parsed item: %+v", got[0])
	}
}

func TestParseStream_CustomComma(t *testing.T) {
	t.Parallel()

	csvData := `name;age;city;active
Alice;25;New York;true
`

	var got []testUser

	err := ParseStream(
		context.Background(),
		strings.NewReader(csvData),
		testUserMapper,
		func(item testUser) error {
			got = append(got, item)
			return nil
		},
		ParseOptions{
			Comma: ';',
		},
	)
	if err != nil {
		t.Fatalf("ParseStream error: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 item, got %d", len(got))
	}
}

func TestParseStream_NilMapper(t *testing.T) {
	t.Parallel()

	err := ParseStream[testUser](
		context.Background(),
		strings.NewReader("name,age\nAlice,25\n"),
		nil,
		func(item testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected nil mapper error")
	}
}

func TestParseStream_NilHandler(t *testing.T) {
	t.Parallel()

	err := ParseStream(
		context.Background(),
		strings.NewReader("name,age\nAlice,25\n"),
		func(row RowAccessor) (testUser, error) {
			return testUser{}, nil
		},
		nil,
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected nil handler error")
	}
}

func TestParseStream_ReadError(t *testing.T) {
	t.Parallel()

	r := &brokenReader{}

	err := ParseStream(
		context.Background(),
		r,
		func(row RowAccessor) (testUser, error) {
			return testUser{}, nil
		},
		func(item testUser) error { return nil },
		ParseOptions{},
	)
	if err == nil {
		t.Fatal("expected read error")
	}
}

type brokenReader struct{}

func (b *brokenReader) Read(p []byte) (int, error) {
	return 0, io.ErrUnexpectedEOF
}
