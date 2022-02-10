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
		mutex:    &sync.Mutex{},
		id:       0,
		enqueued: make(map[ID]*task),
		running:  make(map[ID]*task),
	}

	go func() {
		lastPrint := time.Now().Round(time.Minute)
		for {
			gq.mutex.Lock()
			if time.Since(lastPrint) > time.Second*5 {
				running := 0
				for _, t := range gq.running {
					if t.endTime != nil {
						running++
					}
				}
				lastPrint = time.Now()
			}

			for k, v := range gq.enqueued {
				if len(gq.running) >= rate {
					break
				}

				gq.running[k] = v
				go v.job(k)
			}

			toDelete := make([]ID, 0)
			for k, task := range gq.running {
				delete(gq.enqueued, k)

				if task.endTime != nil && time.Since(*task.endTime) > dur {
					toDelete = append(toDelete, k)
				}
			}

			for _, id := range toDelete {
				delete(gq.running, id)
			}

			gq.mutex.Unlock()
		}
	}()

	return gq
}

func (gq *GQueue) Done(id ID) {
	gq.mutex.Lock()
	task := gq.running[id]
	now := time.Now()
	task.endTime = &now
	gq.mutex.Unlock()
	gq.wg.Done()
}

func (gq *GQueue) Add(t *task) {
	gq.mutex.Lock()
	gq.wg.Add(1)
	gq.enqueued[gq.id] = t
	gq.id++
	gq.mutex.Unlock()
}

func (gq *GQueue) Wait() {
	gq.wg.Wait()
}
