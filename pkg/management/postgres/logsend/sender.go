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

package logsend

import (
	"context"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres/logpipe"
	"golang.org/x/sync/errgroup"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	cnpgiClient "github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/client"
	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/repository"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type LogSender interface {
	Start(ctx context.Context) error
	Write(record logpipe.NamedRecord)
}

type sender struct {
	mgr        manager.Manager
	instance   *postgres.Instance
	repository repository.Interface
	logging    chan *logpipe.LoggingRecord
	pgAudit    chan *logpipe.PgAuditRecord
}

func NewLogSender(
	mgr manager.Manager,
	instance *postgres.Instance,
	repository repository.Interface,
) LogSender {
	return &sender{
		mgr:        mgr,
		instance:   instance,
		repository: repository,
		logging:    make(chan *logpipe.LoggingRecord),
		pgAudit:    make(chan *logpipe.PgAuditRecord),
	}
}

func (s *sender) Write(record logpipe.NamedRecord) {
	switch val := record.(type) {
	case *logpipe.LoggingRecord:
		s.logging <- val
	case *logpipe.PgAuditLoggingDecorator:
		s.logging <- val.LoggingRecord
		if val.Audit != nil {
			s.pgAudit <- val.Audit
		}
	default:
		return
	}
}

func (s *sender) Start(ctx context.Context) error {
	var g errgroup.Group
	var cli cnpgiClient.Client
	var err error

	if cli, err = s.getPluginClient(ctx); err != nil {
		log.FromContext(ctx).Error(err, "failed to create plugin client")
		return err
	}
	defer cli.Close(ctx)

	server := cli.SendLogs(ctx)
	g.Go(func() error { return server.Start(ctx) })
	g.Go(func() error { return chan2stream(ctx, s.logging, NewServerLog, server.Send) })

	defer server.Close(ctx)
	defer close(s.logging)

	return g.Wait()
}

func (s *sender) getPluginClient(ctx context.Context) (cnpgiClient.Client, error) {
	var cluster apiv1.Cluster
	var err error

	if err = s.mgr.GetClient().Get(ctx, types.NamespacedName{
		Name:      s.instance.GetClusterName(),
		Namespace: s.instance.GetNamespaceName(),
	}, &cluster); err != nil {
		log.FromContext(ctx).Error(err, "failed to retrieve Cluster")
		return nil, err
	}

	loadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return cnpgiClient.WithPlugins(
		loadCtx,
		s.repository,
		cluster.GetInstanceEnabledPluginNames()...,
	)
}

func chan2stream[R, S any](ctx context.Context, ch <-chan R, f func(R) S, g func(S) error) error {
	for {
		select {
		case <-ctx.Done():
			return nil // shutting down
		case t, ok := <-ch:
			if !ok {
				return nil // channel was closed
			}
			if err := g(f(t)); err != nil {
				return err
			}
		}
	}
}
