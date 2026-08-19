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

	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/stream"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/management/postgres"
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
	CSV() chan<- []byte
	JSON() chan<- []byte
}

type sender struct {
	mgr        manager.Manager
	instance   *postgres.Instance
	repository repository.Interface
	csv        chan []byte
	json       chan []byte
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
		csv:        make(chan []byte),
		json:       make(chan []byte),
	}
}

func (p *sender) CSV() chan<- []byte {
	return p.csv
}

func (p *sender) JSON() chan<- []byte {
	return p.json
}

func (p *sender) Start(ctx context.Context) error {
	var g errgroup.Group
	var cli cnpgiClient.Client
	var err error

	if cli, err = p.getPluginClient(ctx); err != nil {
		log.FromContext(ctx).Error(err, "failed to create plugin client")
		return err
	}
	defer cli.Close(ctx)

	csv := cli.SendCSVLog(ctx)
	g.Go(func() error { return csv.Start(ctx) })
	g.Go(func() error { return chanToStream(ctx, p.csv, csv) })
	defer csv.Close(ctx)
	defer close(p.csv)

	json := cli.SendJSONLog(ctx)
	g.Go(func() error { return json.Start(ctx) })
	g.Go(func() error { return chanToStream(ctx, p.json, json) })
	defer json.Close(ctx)
	defer close(p.json)

	return g.Wait()
}

func (p *sender) getPluginClient(ctx context.Context) (cnpgiClient.Client, error) {
	var cluster apiv1.Cluster
	var err error

	if err = p.mgr.GetClient().Get(ctx, types.NamespacedName{
		Name:      p.instance.GetClusterName(),
		Namespace: p.instance.GetNamespaceName(),
	}, &cluster); err != nil {
		log.FromContext(ctx).Error(err, "failed to retrieve Cluster")
		return nil, err
	}

	loadCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return cnpgiClient.WithPlugins(
		loadCtx,
		p.repository,
		cluster.GetInstanceEnabledPluginNames()...,
	)
}

func chanToStream(ctx context.Context, ch <-chan []byte, sc stream.StreamingClient) error {
	for {
		select {
		case <-ctx.Done():
			return nil // shutting down
		case line, ok := <-ch:
			if !ok {
				return nil // channel was closed
			}
			if err := sc.Send(line); err != nil {
				return err
			}
		}
	}
}
