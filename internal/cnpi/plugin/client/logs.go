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

package client

import (
	"context"
	"errors"
	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/stream"
	"github.com/cloudnative-pg/cnpg-i/pkg/logs"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc"
	"slices"
)

type LogsCapabilities interface {
	SendCSVLog(ctx context.Context) stream.StreamingClient
	SendJSONLog(ctx context.Context) stream.StreamingClient
}

func (data *data) SendCSVLog(ctx context.Context) stream.StreamingClient {
	clients := data.openStreamingClients(ctx, sendCSVLog)

	return stream.NewStreamingClientProxy(clients...)
}

func (data *data) SendJSONLog(ctx context.Context) stream.StreamingClient {
	clients := data.openStreamingClients(ctx, sendJSONLog)

	return stream.NewStreamingClientProxy(clients...)
}

type streamOpener func(ctx context.Context, client logs.LogsClient) (grpc.ClientStreamingClient[logs.SendLogRequest, logs.SendLogResult], error)

func sendCSVLog(ctx context.Context, client logs.LogsClient) (grpc.ClientStreamingClient[logs.SendLogRequest, logs.SendLogResult], error) {
	return client.SendCSVLog(ctx)
}

func sendJSONLog(ctx context.Context, client logs.LogsClient) (grpc.ClientStreamingClient[logs.SendLogRequest, logs.SendLogResult], error) {
	return client.SendJSONLog(ctx)
}

func (data *data) openStreamingClients(ctx context.Context, opener streamOpener) []stream.StreamingClient {
	var clients []stream.StreamingClient

	for idx := range data.plugins {
		plugin := data.plugins[idx]
		logger := log.FromContext(ctx).WithValues("plugin", plugin.Name())

		if !slices.Contains(plugin.LogsCapabilities(), logs.LogsCapability_RPC_TYPE_JSON_LOGS) {
			logger.Debug("skipping plugin; no logs capability")
			continue
		}

		grpcClient, err := opener(ctx, plugin.LogsClient())
		if err != nil {
			logger.Error(err, "failed to open streaming client")
			continue
		}

		clients = append(clients, stream.NewStreamingClient(&streamingRPC{grpcClient}))
	}

	return clients
}

type streamingRPC struct {
	client grpc.ClientStreamingClient[logs.SendLogRequest, logs.SendLogResult]
}

func (s streamingRPC) Send(a any) error {
	switch lines := a.(type) {
	case [][]byte:
		return s.client.Send(&logs.SendLogRequest{Lines: lines})
	default:
		return errors.New("invalid type")
	}
}

func (s streamingRPC) Close() error {
	return s.Close()
}
