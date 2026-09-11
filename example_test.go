package EasyRoutine_test

import (
	"context"
	"fmt"

	EasyRoutine "github.com/LazyEasyDev/EasyRoutine"
)

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
