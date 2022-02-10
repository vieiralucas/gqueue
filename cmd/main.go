package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/vieiralucas/gqueue"
)

func main() {
	gq := gqueue.New(33, time.Duration(1*time.Second))

	for i := 0; i < 100; i++ {
		func(i int) {
			gq.Add(gqueue.NewTask(func(id gqueue.ID) {
				defer gq.Done(id)
				dur := time.Duration((rand.Int() % 5)) * time.Second
				fmt.Println("Start", i, " dur: ", dur)
				time.Sleep(dur)
				fmt.Println("End", i)
			}))
		}(i)
	}

	gq.Wait()

	fmt.Println("Its over")
}
