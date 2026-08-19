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
	"context"

	"github.com/cloudnative-pg/machinery/pkg/log"
)

type StreamingRPC interface {
	Send(any) error
	Close() error
}

type StreamingClient interface {
	Start(ctx context.Context) error
	Send([]byte) error
	Close(ctx context.Context)
}

type client struct {
	rpc    StreamingRPC
	buffer BoundedByteBuffer
	metric StreamingMetric
}

func NewStreamingClient(rpc StreamingRPC) StreamingClient {
	return &client{
		rpc:    rpc,
		buffer: NewBoundedByteBuffer(10 * 1024 * 1024),
		metric: &noopLogSenderMetric{},
	}
}

func (s *client) Start(ctx context.Context) error {
	defer func() { _ = s.rpc.Close() }()
	send := func() error {
		lines := s.buffer.Drain()
		if len(lines) == 0 {
			return nil
		}
		return s.rpc.Send(lines)
	}
	for {
		select {
		case <-s.buffer.Wait():
			if err := send(); err != nil {
				return err
			}
		case <-ctx.Done():
			return send()
		}
	}
}

func (s *client) Send(line []byte) error {
	if s.buffer.Push(line) {
		s.metric.IncrementAccepted()
	} else {
		s.metric.IncrementDropped()
	}
	return nil
}

func (s *client) Close(ctx context.Context) {
	if err := s.rpc.Close(); err != nil {
		log.FromContext(ctx).Error(err, "when closing streaming client")
	}
}
