# csvx

`csvx` — это быстрый и удобный пакет для потокового парсинга CSV в Go

## 📦 Установка

```bash
go get github.com/exgamer/csvx
```

---

## 📄 Пример CSV

```csv
name,age,city,active
Alice,25,New York,true
Bob,30,Los Angeles,false
```

---

## 🔧 Базовое использование

```go

ctx := context.Background()

file, err := os.Open("test.csv")

if err != nil {
    log.Fatal(err)
}

defer file.Close()

mapper := func(row csvx.RowAccessor) (User, error) {
    name, err := row.Required("name")
    if err != nil {
        return User{}, err
    }

    age, err := row.Int("age")
    if err != nil {
        return User{}, err
    }

    active, err := row.Bool("active")
    if err != nil {
        return User{}, err
    }

    return User{
        Name:   name,
        Age:    age,
        City:   row.String("city"),
        Active: active,
    }, nil
}
```

---

## 📥 Потоковый парсинг

```go
err := csvx.ParseStream(ctx, file, mapper, func(user User) error {
    fmt.Println(user)
    return nil
}, csvx.ParseOptions{})
```

---

## 📦 Batch обработка (рекомендуется для больших файлов)

```go
err := csvx.ParseInBatches(
    ctx,
    file,
    mapper,
    1000,
    func(batch []User) error {
        log.Printf("save batch: %d", len(batch))
		
        return nil
    },
    csvx.ParseOptions{
        TrimLeadingSpace:  true,
        TrimHeaderSpace:   true,
        TrimUTF8BOM:       true,
        AllowShortRows:    true,
        SkipDecodeErrors:  true,
        SkipHandlerErrors: false,
        MaxRowErrors:      100,
        OnRowError: func(err *csvx.RowError) {
        log.Printf("row error: %v", err)
        },
    },
)

```

---

## 🔄 Через канал

```go
items, errs := csvx.ParseStreamToChannel(ctx, file, mapper, csvx.ParseOptions{}, 100)

for item := range items {
    fmt.Println(item)
}

if err := <-errs; err != nil {
    log.Fatal(err)
}
```

---

## ⚙️ Опции

```go
csvx.ParseOptions{
    Comma:               ',',
    LazyQuotes:          true,
    TrimLeadingSpace:    true,
    AllowShortRows:      true,
    TrimHeaderSpace:     true,
    TrimUTF8BOM:         true,
    SkipDecodeErrors:    true,
    SkipHandlerErrors:   false,
    MaxRowErrors:        100,
}
```

---

## ⚠️ Важно про память

`csvx` использует:

```go
reader.ReuseRecord = true
```

Это значит:

- строки **переиспользуются**
- нельзя сохранять `RawValues()` после обработки

### ❌ Нельзя

```go
saved = append(saved, row.RawValues())
```

### ✅ Можно

```go
saved = append(saved, row.ValuesCopy())
```

---
### Тесты 

```bash
go test -v ./internal/csvx

go test -bench=. ./internal/csvx

```

## 📜 Лицензия

MIT
