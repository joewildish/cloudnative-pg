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
	"errors"
	"sync"
)

type proxy struct {
	clients []StreamingClient
}

func NewStreamingClientProxy(proxied ...StreamingClient) StreamingClient {
	return &proxy{clients: proxied}
}

func (p *proxy) Start(ctx context.Context) error {
	var g sync.WaitGroup
	var mu sync.Mutex
	var errs []error

	for _, p := range p.clients {
		g.Go(func() {
			if err := p.Start(ctx); err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		})
	}
	g.Wait()
	return errors.Join(errs...)
}

func (p *proxy) SendServerLog(log ServerLog) error {
	var errs []error

	for _, cl := range p.clients {
		errs = append(errs, cl.SendServerLog(log))
	}
	return errors.Join(errs...)
}

func (p *proxy) SendAuditLog(log AuditLog) error {
	var errs []error

	for _, cl := range p.clients {
		errs = append(errs, cl.SendAuditLog(log))
	}
	return errors.Join(errs...)
}

func (p *proxy) Close(ctx context.Context) {
	for _, cl := range p.clients {
		cl.Close(ctx)
	}
}
