package EasyRoutine_test

import (
	"context"
	"fmt"
	"time"

	EasyRoutine "github.com/LazyEasyDev/EasyRoutine"
)

type exampleLease struct{}

func (exampleLease) Action(context.Context, EasyRoutine.LeaseAction, EasyRoutine.LeaseState) bool {
	return true
}

func (exampleLease) GetLogs(context.Context, ...string) ([]EasyRoutine.SupervisorLog, error) {
	return nil, nil
}

func (exampleLease) GetStatuses(context.Context, ...string) ([]EasyRoutine.SupervisorStatus, error) {
	return nil, nil
}

func ExampleSafeGo() {
	attempts := 0
	handle, err := EasyRoutine.SafeGo(context.Background(), func(context.Context) {
		attempts++
		if attempts == 1 {
			panic("temporary failure")
		}
		fmt.Println("working")
	}, func(recovered EasyRoutine.Panic, failures int) EasyRoutine.PanicDecision {
		fmt.Printf("recovered %v (failure %d)\n", recovered.Value, failures)
		return EasyRoutine.PanicDecision{Retry: true}
	})
	if err != nil {
		panic(err)
	}
	handle.Wait()
	// Output:
	// recovered temporary failure (failure 1)
	// working
}

func ExampleStartUniqueSupervisor() {
	if err := EasyRoutine.InitLease(exampleLease{}); err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	supervisor, err := EasyRoutine.StartUniqueSupervisor(ctx, "reports", func(ctx context.Context) {
		fmt.Println("unique work")
		cancel()
	}, func(EasyRoutine.Panic) {}, time.Minute)
	if err != nil {
		panic(err)
	}
	supervisor.Wait()
	// Output: unique work
}
