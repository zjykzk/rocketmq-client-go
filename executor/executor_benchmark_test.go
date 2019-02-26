package executor

import (
	"testing"
	"time"
)

const (
	runTimes      = 1000000
	benchParam    = 10
	benchPoolSize = 200000
)

type funcRunnbale func()

func (d funcRunnbale) Run() {
	d()
}

func demoPoolFunc() {
	time.Sleep(time.Duration(benchParam) * time.Millisecond)
}

func BenchmarkGoroutine(b *testing.B) {
	for i := 0; i < b.N; i++ {
		for j := 0; j < runTimes; j++ {
			go demoPoolFunc()
		}
	}
}

func BenchmarkPool(b *testing.B) {
	p, _ := NewPoolExecutor(
		"benchmark", benchPoolSize, benchPoolSize, time.Minute, NewLinkedBlockingQueue())
	defer p.Shutdown()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < runTimes; j++ {
			p.Execute(funcRunnbale(demoPoolFunc))
		}
	}
	b.StopTimer()
}

func BenchmarkSemaphore(b *testing.B) {
	sema := make(chan struct{}, benchPoolSize)
	for i := 0; i < b.N; i++ {
		for j := 0; j < runTimes; j++ {
			sema <- struct{}{}
			go func() {
				demoPoolFunc()
				<-sema
			}()
		}
	}
}
