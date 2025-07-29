package rabbitevents

import (
	"context"
	"encoding/json"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RabbitEvents", func() {
	Describe("Change/SubscribeUnfiltered", func() {
		It("Should get a message sent", func() {
			eo := &eventObserver{}
			var lastEvent *Event
			eo.SubscribeUnfiltered(func(e *Event) { lastEvent = e })

			ctx := context.Background()
			event := &Event{
				Path:   "p",
				Action: Create,
				ID:     1,
				Source: EventSource{},
				Old:    nil,
				State:  nil,
			}

			data, err := json.Marshal(event)
			Expect(err).To(BeNil())

			err = eo.Change(ctx, data)
			Expect(err).To(BeNil())

			Expect(lastEvent).To(Equal(event))
		})

		It("Should handle concurrency", func() {
			eo := &eventObserver{}

			ctx := context.Background()
			event := &Event{
				Path:   "p",
				Action: Create,
				ID:     1,
				Source: EventSource{},
				Old:    nil,
				State:  nil,
			}

			data, err := json.Marshal(event)
			Expect(err).To(BeNil())

			events := make([]*Event, 0)
			var m sync.Mutex
			eo.SubscribeUnfiltered(func(e *Event) {
				m.Lock()
				defer m.Unlock()
				events = append(events, e)
			})

			var wg sync.WaitGroup

			wg.Add(10)
			for k := 0; k < 10; k++ {
				go func() {
					defer wg.Done()
					eo.SubscribeUnfiltered(func(e *Event) {})
				}()
			}

			wg.Add(10)
			for k := 0; k < 10; k++ {
				go func() {
					defer wg.Done()
					err := eo.Change(ctx, data)
					Expect(err).To(BeNil())
				}()
			}

			wg.Wait()

			Expect(events).To(HaveLen(10))
		})
	})

	Describe("Change/Subscribe", func() {
		It("Should get a message sent", func() {
			eo := &eventObserver{}
			var lastEvent *Event
			eo.Subscribe([]int64{1}, func(e *Event) { lastEvent = e })

			ctx := context.Background()
			event := &Event{
				Path:   "p",
				Action: Create,
				ID:     1,
				Source: EventSource{},
				Old:    nil,
				State:  nil,
			}

			data, err := json.Marshal(event)
			Expect(err).To(BeNil())

			err = eo.Change(ctx, data)
			Expect(err).To(BeNil())

			Expect(lastEvent).To(Equal(event))
		})

		It("Should handle concurrency", func() {
			eo := &eventObserver{}

			ctx := context.Background()
			event := &Event{
				Path:   "p",
				Action: Create,
				ID:     1,
				Source: EventSource{},
				Old:    nil,
				State:  nil,
			}

			data, err := json.Marshal(event)
			Expect(err).To(BeNil())

			event2 := &Event{
				Path:   "p",
				Action: Create,
				ID:     2,
				Source: EventSource{},
				Old:    nil,
				State:  nil,
			}

			data2, err := json.Marshal(event2)
			Expect(err).To(BeNil())

			events := make([]*Event, 0)
			var m sync.Mutex
			eo.Subscribe([]int64{1}, func(e *Event) {
				m.Lock()
				defer m.Unlock()
				events = append(events, e)
			})

			var wg sync.WaitGroup

			wg.Add(10)
			for k := 0; k < 10; k++ {
				go func() {
					defer wg.Done()
					eo.Subscribe([]int64{1}, func(e *Event) {})
				}()
			}

			wg.Add(10)
			for k := 0; k < 10; k++ {
				go func() {
					defer wg.Done()
					err := eo.Change(ctx, data)
					Expect(err).To(BeNil())
				}()
			}

			wg.Add(10)
			for k := 0; k < 10; k++ {
				go func() {
					defer wg.Done()
					err := eo.Change(ctx, data2)
					Expect(err).To(BeNil())
				}()
			}

			wg.Wait()

			Expect(events).To(HaveLen(10))
		})
	})
})
