package EasyRoutine_test

import (
	"context"
	"fmt"
	"time"

	EasyRoutine "github.com/LazyEasyDev/EasyRoutine"
)

type exampleLease struct{}

func (exampleLease) Acquire(context.Context, string, string, time.Duration) bool {
	return true
}

func (exampleLease) Renew(context.Context, string, string, time.Duration) bool {
	return true
}

func (exampleLease) Release(context.Context, string, string) bool {
	return true
}

func ExampleGo() {
	handle := EasyRoutine.Go(context.Background(), func(context.Context) {
		fmt.Println("working")
	}, func(recovered EasyRoutine.Panic) EasyRoutine.PanicRetry {
		fmt.Printf("recovered: %v\n", recovered.Value)
		return EasyRoutine.PanicRetry90s
	})
	handle.Wait()
	// Output: working
}

func ExampleStartUniqueSupervisor() {
	if err := EasyRoutine.InitLease(exampleLease{}); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	supervisor, err := EasyRoutine.StartUniqueSupervisor(ctx, "reports", func(context.Context) {
		fmt.Println("unique work")
		cancel()
	}, nil)
	if err != nil {
		panic(err)
	}
	supervisor.Wait()
	// Output: unique work
}
