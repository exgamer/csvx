package csvx

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// --- helpers ---

func generateCSV(rows int) string {
	var b strings.Builder

	b.WriteString("name,age,city,active\n")

	for i := 0; i < rows; i++ {
		b.WriteString(fmt.Sprintf(
			"User%d,%d,City%d,%t\n",
			i,
			20+i%50,
			i%100,
			i%2 == 0,
		))
	}

	return b.String()
}

type benchUser struct {
	Name   string
	Age    int
	City   string
	Active bool
}

func benchMapper(row RowAccessor) (benchUser, error) {
	age, err := row.Int("age")
	if err != nil {
		return benchUser{}, err
	}

	active, err := row.Bool("active")
	if err != nil {
		return benchUser{}, err
	}

	return benchUser{
		Name:   row.String("name"),
		Age:    age,
		City:   row.String("city"),
		Active: active,
	}, nil
}

func noopHandler[T any](T) error { return nil }

func batchHandler[T any](_ []T) error { return nil }

func BenchmarkParseStream_1k(b *testing.B) {
	data := generateCSV(1_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ParseStream(
			context.Background(),
			strings.NewReader(data),
			benchMapper,
			noopHandler[benchUser],
			ParseOptions{},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseStream_100k(b *testing.B) {
	data := generateCSV(100_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ParseStream(
			context.Background(),
			strings.NewReader(data),
			benchMapper,
			noopHandler[benchUser],
			ParseOptions{},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseInBatches_1k_batch100(b *testing.B) {
	data := generateCSV(1_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ParseInBatches(
			context.Background(),
			strings.NewReader(data),
			benchMapper,
			100,
			batchHandler[benchUser],
			ParseOptions{},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseInBatches_100k_batch1000(b *testing.B) {
	data := generateCSV(100_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := ParseInBatches(
			context.Background(),
			strings.NewReader(data),
			benchMapper,
			1000,
			batchHandler[benchUser],
			ParseOptions{},
		)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseStreamToChannel_10k(b *testing.B) {
	data := generateCSV(10_000)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		items, errs := ParseStreamToChannel(
			context.Background(),
			strings.NewReader(data),
			benchMapper,
			ParseOptions{},
			100,
		)

		for range items {
		}

		if err := <-errs; err != nil {
			b.Fatal(err)
		}
	}
}
