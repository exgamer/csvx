package csvx

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"
)

// HeaderIndex - индекс заголовков CSV.
type HeaderIndex map[string]int

// RowAccessor - быстрый доступ к данным строки без map[string]string на каждую запись.
//
// Важно:
//   - accessor живет только в рамках текущего вызова mapper/handler
//   - нельзя сохранять RowAccessor для последующего использования
//   - при включенном csv.Reader.ReuseRecord данные строки могут быть переиспользованы
//     следующим вызовом Read()
type RowAccessor struct {
	index HeaderIndex
	row   []string
}

// NewRowAccessor - создать accessor для строки.
func NewRowAccessor(index HeaderIndex, row []string) RowAccessor {
	return RowAccessor{
		index: index,
		row:   row,
	}
}

// HasColumn - проверить наличие заголовка.
func (r RowAccessor) HasColumn(key string) bool {
	_, ok := r.index[key]
	return ok
}

// Get - вернуть значение по ключу.
// Если ключа нет или индекс выходит за границы строки, вернет пустую строку.
func (r RowAccessor) Get(key string) string {
	idx, ok := r.index[key]
	if !ok || idx >= len(r.row) {
		return ""
	}

	return r.row[idx]
}

// String - вернуть trim-space значение по ключу.
func (r RowAccessor) String(key string) string {
	return strings.TrimSpace(r.Get(key))
}

// Required - вернуть обязательное поле.
func (r RowAccessor) Required(key string) (string, error) {
	idx, ok := r.index[key]
	if !ok {
		return "", fmt.Errorf("field %q not found", key)
	}

	if idx >= len(r.row) {
		return "", fmt.Errorf("field %q is empty", key)
	}

	value := strings.TrimSpace(r.row[idx])
	if value == "" {
		return "", fmt.Errorf("field %q is empty", key)
	}

	return value, nil
}

// Int - распарсить int.
func (r RowAccessor) Int(key string) (int, error) {
	value, err := r.Required(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("field %q parse int: %w", key, err)
	}

	return result, nil
}

// Int64 - распарсить int64.
func (r RowAccessor) Int64(key string) (int64, error) {
	value, err := r.Required(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q parse int64: %w", key, err)
	}

	return result, nil
}

// Uint - распарсить uint.
func (r RowAccessor) Uint(key string) (uint, error) {
	value, err := r.Required(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q parse uint: %w", key, err)
	}

	if strconv.IntSize == 32 && result > math.MaxUint32 {
		return 0, fmt.Errorf("field %q overflows uint", key)
	}

	return uint(result), nil
}

// Float64 - распарсить float64.
func (r RowAccessor) Float64(key string) (float64, error) {
	value, err := r.Required(key)
	if err != nil {
		return 0, err
	}

	result, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("field %q parse float64: %w", key, err)
	}

	return result, nil
}

// Bool - распарсить bool.
func (r RowAccessor) Bool(key string) (bool, error) {
	value, err := r.Required(key)
	if err != nil {
		return false, err
	}

	result, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return false, fmt.Errorf("field %q parse bool: %w", key, err)
	}

	return result, nil
}

// RawValues - вернуть исходную строку без копирования.
//
// Важно:
//   - возвращенный slice нельзя изменять
//   - нельзя сохранять его после завершения текущего mapper/handler вызова
//   - при ReuseRecord=true данные могут быть переиспользованы следующим Read()
func (r RowAccessor) RawValues() []string {
	return r.row
}

// ValuesCopy - вернуть безопасную копию строки.
func (r RowAccessor) ValuesCopy() []string {
	result := make([]string, len(r.row))
	copy(result, r.row)
	return result
}

// RowMapper - преобразует RowAccessor в T.
type RowMapper[T any] func(row RowAccessor) (T, error)

// RowHandler - вызывается для каждой записи.
type RowHandler[T any] func(item T) error

// BatchHandler - вызывается для батча записей.
type BatchHandler[T any] func(batch []T) error

// RowError - ошибка строки CSV.
type RowError struct {
	Line int
	Err  error
}

func (e *RowError) Error() string {
	return fmt.Sprintf("csv line %d: %v", e.Line, e.Err)
}

func (e *RowError) Unwrap() error {
	return e.Err
}

// RowErrorHandler - callback для обработки ошибок строки.
type RowErrorHandler func(err *RowError)

// ParseOptions - настройки парсинга CSV.
type ParseOptions struct {
	// Разделитель. По умолчанию ','.
	Comma rune

	// Разрешать "грязные" кавычки.
	LazyQuotes bool

	// Убирать пробелы после разделителя.
	TrimLeadingSpace bool

	// Разрешать меньше колонок, чем в headers.
	AllowShortRows bool

	// Ошибка, если колонок больше чем headers.
	DisallowExtraColumns bool

	// Разрешать пустые имена заголовков.
	AllowEmptyHeaders bool

	// Обрезать пробелы у headers.
	TrimHeaderSpace bool

	// Пропускать BOM в первом заголовке.
	TrimUTF8BOM bool

	// Пропускать ошибки чтения/сборки row/mapper и продолжать импорт.
	SkipDecodeErrors bool

	// Пропускать ошибки handler и продолжать импорт.
	SkipHandlerErrors bool

	// Максимальное количество ошибок строк.
	// 0 = без лимита.
	MaxRowErrors int

	// Callback на ошибку строки.
	OnRowError RowErrorHandler
}

// ParseFileStream - потоковый парсинг CSV файла.
func ParseFileStream[T any](
	ctx context.Context,
	filePath string,
	mapper RowMapper[T],
	handler RowHandler[T],
	opts ParseOptions,
) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	return ParseStream(ctx, file, mapper, handler, opts)
}

// ParseStream - потоковый парсинг CSV из io.Reader.
func ParseStream[T any](
	ctx context.Context,
	r io.Reader,
	mapper RowMapper[T],
	handler RowHandler[T],
	opts ParseOptions,
) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if mapper == nil {
		return errors.New("mapper is nil")
	}
	if handler == nil {
		return errors.New("handler is nil")
	}

	reader := csv.NewReader(r)

	if opts.Comma != 0 {
		reader.Comma = opts.Comma
	}

	reader.LazyQuotes = opts.LazyQuotes
	reader.TrimLeadingSpace = opts.TrimLeadingSpace
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	headers, err := reader.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("csv is empty")
		}

		return fmt.Errorf("read headers: %w", err)
	}

	headers = normalizeHeaders(headers, opts)

	if err := validateHeaders(headers, opts); err != nil {
		return fmt.Errorf("validate headers: %w", err)
	}

	index := buildHeaderIndex(headers)

	lineNumber := 2
	rowErrorCount := 0

	handleRowError := func(line int, err error, skip bool) error {
		rowErr := &RowError{
			Line: line,
			Err:  err,
		}

		if !skip {
			return rowErr
		}

		rowErrorCount++
		callRowErrorHandler(opts.OnRowError, rowErr)

		if opts.MaxRowErrors > 0 && rowErrorCount >= opts.MaxRowErrors {
			return fmt.Errorf("max row errors reached: %d: %w", rowErrorCount, rowErr)
		}

		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		record, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			if err := handleRowError(lineNumber, err, opts.SkipDecodeErrors); err != nil {
				return err
			}
			lineNumber++
			continue
		}

		if err := validateRecord(headers, record, opts); err != nil {
			if err := handleRowError(lineNumber, err, opts.SkipDecodeErrors); err != nil {
				return err
			}
			lineNumber++
			continue
		}

		item, err := mapper(NewRowAccessor(index, record))
		if err != nil {
			if err := handleRowError(lineNumber, err, opts.SkipDecodeErrors); err != nil {
				return err
			}
			lineNumber++
			continue
		}

		if err := handler(item); err != nil {
			if err := handleRowError(lineNumber, err, opts.SkipHandlerErrors); err != nil {
				return err
			}
			lineNumber++
			continue
		}

		lineNumber++
	}
}

// ParseFileToSlice - собрать CSV в []T.
//
// Использовать только для небольших файлов.
// Для больших файлов используйте ParseStream или ParseInBatches.
func ParseFileToSlice[T any](
	ctx context.Context,
	filePath string,
	mapper RowMapper[T],
	opts ParseOptions,
) ([]T, error) {
	result := make([]T, 0, 128)

	err := ParseFileStream(ctx, filePath, mapper, func(item T) error {
		result = append(result, item)
		return nil
	}, opts)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// ParseStreamToChannel - потоковый парсинг в канал.
func ParseStreamToChannel[T any](
	ctx context.Context,
	r io.Reader,
	mapper RowMapper[T],
	opts ParseOptions,
	buffer int,
) (<-chan T, <-chan error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if buffer <= 0 {
		buffer = 1
	}

	out := make(chan T, buffer)
	errCh := make(chan error, 1)

	go func() {
		defer close(out)
		defer close(errCh)

		err := ParseStream(ctx, r, mapper, func(item T) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case out <- item:
				return nil
			}
		}, opts)

		if err != nil {
			errCh <- err
		}
	}()

	return out, errCh
}

// ParseFileInBatches - потоково парсит CSV из файла и передает записи батчами.
func ParseFileInBatches[T any](
	ctx context.Context,
	filePath string,
	mapper RowMapper[T],
	batchSize int,
	batchHandler BatchHandler[T],
	opts ParseOptions,
) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	return ParseInBatches(ctx, file, mapper, batchSize, batchHandler, opts)
}

// ParseInBatches - потоково парсит CSV из io.Reader и передает записи батчами.
func ParseInBatches[T any](
	ctx context.Context,
	r io.Reader,
	mapper RowMapper[T],
	batchSize int,
	batchHandler BatchHandler[T],
	opts ParseOptions,
) error {
	if batchSize <= 0 {
		return errors.New("batchSize must be greater than 0")
	}

	if batchHandler == nil {
		return errors.New("batchHandler is nil")
	}

	batch := make([]T, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}

		chunk := append([]T(nil), batch...)

		if err := batchHandler(chunk); err != nil {
			return err
		}

		batch = batch[:0]
		return nil
	}

	err := ParseStream(ctx, r, mapper, func(item T) error {
		batch = append(batch, item)

		if len(batch) >= batchSize {
			return flush()
		}

		return nil
	}, opts)
	if err != nil {
		return err
	}

	return flush()
}

func normalizeHeaders(headers []string, opts ParseOptions) []string {
	result := make([]string, len(headers))
	copy(result, headers)

	if opts.TrimUTF8BOM && len(result) > 0 {
		result[0] = trimUTF8BOM(result[0])
	}

	for i := range result {
		if opts.TrimHeaderSpace {
			result[i] = strings.TrimSpace(result[i])
		}
	}

	return result
}

func validateHeaders(headers []string, opts ParseOptions) error {
	if len(headers) == 0 {
		return errors.New("headers are empty")
	}

	seen := make(map[string]struct{}, len(headers))

	for i, header := range headers {
		if header == "" && !opts.AllowEmptyHeaders {
			return fmt.Errorf("header at index %d is empty", i)
		}

		if _, exists := seen[header]; exists {
			return fmt.Errorf("duplicate header %q", header)
		}

		seen[header] = struct{}{}
	}

	return nil
}

func buildHeaderIndex(headers []string) HeaderIndex {
	result := make(HeaderIndex, len(headers))
	for i, header := range headers {
		result[header] = i
	}

	return result
}

func validateRecord(headers, record []string, opts ParseOptions) error {
	if opts.DisallowExtraColumns && len(record) > len(headers) {
		return fmt.Errorf("too many columns: got %d, expected %d", len(record), len(headers))
	}

	if !opts.AllowShortRows && len(record) < len(headers) {
		return fmt.Errorf("too few columns: got %d, expected %d", len(record), len(headers))
	}

	return nil
}

func trimUTF8BOM(value string) string {
	if value == "" {
		return value
	}

	if !utf8.ValidString(value) {
		return value
	}

	const bom = "\uFEFF"
	return strings.TrimPrefix(value, bom)
}

func callRowErrorHandler(handler RowErrorHandler, err *RowError) {
	if handler != nil {
		handler(err)
	}
}
