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
)

type BoundedBuffer[T any] interface {
	Wait() <-chan struct{}
	IsEmpty() bool
	Push(T) bool
	Drain() []T
}

type BoundedByteBuffer = BoundedBuffer[[]byte]

func NewBoundedByteBuffer(maxBytes int) BoundedByteBuffer {
	return newBoundedBuffer[[]byte](func(b []byte) int { return len(b) }, maxBytes)
}

type buf[T any] struct {
	mu sync.Mutex
	ch chan struct{}

	items   []T         // items held within the buffer
	count   int         // number of items within the buffer
	size    int         // cumulative size of the items within the buffer
	maxSize int         // maximum cumulative size allowed within the buffer
	sizeF   func(T) int // size calculation for an item
}

func newBoundedBuffer[T any](sizeF func(T) int, maxSize int) BoundedBuffer[T] {
	return &buf[T]{
		ch:      make(chan struct{}, 1),
		items:   make([]T, 16),
		maxSize: maxSize,
		sizeF:   sizeF,
	}
}

func (b *buf[T]) Wait() <-chan struct{} {
	return b.ch
}

func (b *buf[T]) IsEmpty() bool {
	return b.count == 0
}

func (b *buf[T]) Push(item T) bool {
	size := b.sizeF(item)

	b.mu.Lock()
	defer b.mu.Unlock()

	if size < 0 || size > b.maxSize-b.size {
		return false
	}

	// grow the items slice if necessary
	if b.count == len(b.items) {
		items := make([]T, len(b.items)*2)
		copy(items, b.items)
		b.items = nil // GC
		b.items = items
	}

	b.items[b.count] = item
	b.count += 1
	b.size += size

	select {
	case b.ch <- struct{}{}:
		// signal any waiter
	default:
		// but don't wait for the waiter
	}

	return true
}

func (b *buf[T]) Drain() []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	items := make([]T, b.count)
	copy(items, b.items[0:b.count])
	clear(b.items)
	b.count = 0
	b.size = 0

	return items
}
