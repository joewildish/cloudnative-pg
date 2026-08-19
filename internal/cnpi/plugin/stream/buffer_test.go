/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package stream

import (
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("BoundedByteBuffer", func() {
	It("is initially empty", func() {
		b := NewBoundedByteBuffer(10)

		Expect(b.IsEmpty()).To(BeTrue())
		Expect(b.Drain()).To(BeEmpty())
	})

	It("allows empty items", func() {
		b := NewBoundedByteBuffer(0)

		Expect(b.Push([]byte{})).To(BeTrue())
		Expect(b.IsEmpty()).To(BeFalse())

		Expect(b.Drain()).To(Equal([][]byte{
			{},
		}))
	})

	It("accepts items while within the maximum size", func() {
		b := NewBoundedByteBuffer(6)

		Expect(b.Push([]byte("foo"))).To(BeTrue())
		Expect(b.Push([]byte("bar"))).To(BeTrue())
		Expect(b.IsEmpty()).To(BeFalse())

		Expect(b.Drain()).To(Equal([][]byte{
			[]byte("foo"),
			[]byte("bar"),
		}))
		Expect(b.IsEmpty()).To(BeTrue())
	})

	It("rejects an item that would exceed the maximum size", func() {
		b := NewBoundedByteBuffer(10)

		Expect(b.Push([]byte("123456"))).To(BeTrue())
		Expect(b.Push([]byte("12345"))).To(BeFalse())

		Expect(b.Drain()).To(Equal([][]byte{
			[]byte("123456"),
		}))
	})

	It("allows an item that exactly fills the items", func() {
		b := NewBoundedByteBuffer(10)

		Expect(b.Push([]byte("1234567890"))).To(BeTrue())
		Expect(b.IsEmpty()).To(BeFalse())

		Expect(b.Drain()).To(Equal([][]byte{
			[]byte("1234567890"),
		}))
	})

	It("drains all items and resets the items", func() {
		b := NewBoundedByteBuffer(10)

		Expect(b.Push([]byte("foo"))).To(BeTrue())
		Expect(b.Push([]byte("bar"))).To(BeTrue())
		Expect(b.Drain()).To(Equal([][]byte{
			[]byte("foo"),
			[]byte("bar"),
		}))
		Expect(b.IsEmpty()).To(BeTrue())

		// should be available again after draining.
		Expect(b.Push([]byte("1234567890"))).To(BeTrue())
	})

	It("underlying items grows beyond its initial capacity", func() {
		b := NewBoundedByteBuffer(1024)

		Expect(b.(*buf[[]byte]).items).To(HaveLen(16))
		for i := 0; i < 17; i++ {
			Expect(b.Push([]byte("x"))).To(BeTrue())
		}

		Expect(b.(*buf[[]byte]).items).To(HaveLen(32))
		for i := 0; i < 17; i++ {
			Expect(b.Push([]byte("x"))).To(BeTrue())
		}

		Expect(b.(*buf[[]byte]).items).To(HaveLen(64))

		Expect(b.Drain()).To(HaveLen(17 + 17))
	})

	It("supports concurrent pushes", func() {
		b := NewBoundedByteBuffer(1000)

		const numOfGoRoutines = 20
		const numOfPushesPerGoRoutine = 10

		var wg sync.WaitGroup
		wg.Add(numOfGoRoutines)

		for i := 0; i < numOfGoRoutines; i++ {
			go func() {
				defer wg.Done()

				for j := 0; j < numOfPushesPerGoRoutine; j++ {
					Expect(b.Push([]byte("x"))).To(BeTrue())
				}
			}()
		}

		wg.Wait()

		Expect(b.Drain()).To(HaveLen(numOfGoRoutines * numOfPushesPerGoRoutine))
	})

	It("signals awaiting goroutines when mutated", func() {
		b := NewBoundedByteBuffer(512)

		var wg sync.WaitGroup
		wg.Add(2)

		producer := func() {
			defer wg.Done()
			b.Push([]byte("A"))
		}
		consumer := func() {
			defer wg.Done()
			select {
			case <-b.Wait():
				Expect(b.Drain()).To(Equal([][]byte{[]byte("A")}))
			}
		}

		go producer()
		go consumer()

		wg.Wait()
	})
})
