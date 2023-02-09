# AsyncRedis

### benchmark
```bash
$  go test -bench=. -benchmem -benchtime=10s ./... -timeout 20s
goos: windows
goarch: amd64
pkg: github.com/ywh147906/AsyncRedis
cpu: AMD Ryzen 5 3600X 6-Core Processor
BenchmarkAsyncDo-12       259185             55489 ns/op             232 B/op         10 allocs/op
BenchmarkPoolDo-12        302683             55645 ns/op             136 B/op          8 allocs/op
BenchmarkGoRedis-12       233706             53985 ns/op             474 B/op         13 allocs/op
PASS
ok      github.com/ywh147906/AsyncRedis   45.488s

```

