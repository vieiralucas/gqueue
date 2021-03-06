package gqueue

import (
	"sync"
	"time"
)

type ID int

type task struct {
	job     func(id ID)
	endTime *time.Time
}

func NewTask(job func(id ID)) *task {
	return &task{
		job:     job,
		endTime: nil,
	}
}

type GQueue struct {
	wg   *sync.WaitGroup
	rate int
	dur  time.Duration
	done bool

	mutex    *sync.Mutex
	id       ID
	enqueued map[ID]*task
	running  map[ID]*task
}

func New(rate int, dur time.Duration) *GQueue {
	gq := &GQueue{
		wg:       &sync.WaitGroup{},
		rate:     rate,
		dur:      dur,
		done:     false,
		mutex:    &sync.Mutex{},
		id:       0,
		enqueued: make(map[ID]*task),
		running:  make(map[ID]*task),
	}

	go func() {
		for {
			if gq.done {
				return
			}

			gq.mutex.Lock()

			for k, v := range gq.enqueued {
				if len(gq.running) >= rate {
					break
				}

				gq.running[k] = v
				go v.job(k)
			}

			toDelete := make([]ID, 0)
			sleep := dur
			for k, task := range gq.running {
				delete(gq.enqueued, k)

				if task.endTime != nil {
					ellapsed := time.Since(*task.endTime)

					if ellapsed > dur {
						toDelete = append(toDelete, k)
					} else {
						remaining := dur - ellapsed
						if remaining < sleep {
							sleep = remaining
						}
					}
				}
			}

			for _, id := range toDelete {
				delete(gq.running, id)
			}

			gq.mutex.Unlock()

			if len(gq.running) >= rate {
				time.Sleep(sleep)
			}
		}
	}()

	return gq
}

func (gq *GQueue) Done(id ID) {
	if gq.done {
		return
	}

	gq.mutex.Lock()
	task := gq.running[id]
	now := time.Now()
	task.endTime = &now
	gq.mutex.Unlock()
	gq.wg.Done()
}

func (gq *GQueue) Add(t *task) {
	if gq.done {
		panic("Called Add in a done GQueue")
	}

	gq.mutex.Lock()
	gq.wg.Add(1)
	gq.enqueued[gq.id] = t
	gq.id++
	gq.mutex.Unlock()
}

func (gq *GQueue) Wait() {
	gq.wg.Wait()
	gq.done = true
}
