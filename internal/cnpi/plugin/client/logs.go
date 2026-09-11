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
	"slices"

	"github.com/cloudnative-pg/cloudnative-pg/internal/cnpi/plugin/stream"
	"github.com/cloudnative-pg/cnpg-i/pkg/logs"
	"github.com/cloudnative-pg/machinery/pkg/log"
	"google.golang.org/grpc"
)

type LogsCapabilities interface {
	UploadServerLogs(ctx context.Context) stream.StreamingClient
	UploadAuditLogs(ctx context.Context) stream.StreamingClient
}

func (data *data) UploadServerLogs(ctx context.Context) stream.StreamingClient {
	var clients []stream.StreamingClient

	for idx := range data.plugins {
		plugin := data.plugins[idx]
		logger := log.FromContext(ctx).WithValues("plugin", plugin.Name())

		if slices.Contains(plugin.LogsCapabilities(), logs.LogsCapability_RPC_TYPE_SERVER_LOGS) {
			grpcClient, err := plugin.LogsClient().UploadServerLogs(ctx)
			if err != nil {
				logger.Error(err, "failed to open streaming client")
				continue
			}
			rpc := &streamingRPC{
				hasServerLogCapability: true,
				serverLogs:             grpcClient,
			}
			clients = append(clients, stream.NewStreamingClient(rpc))
		}
	}

	return stream.NewStreamingClientProxy(clients...)
}

func (data *data) UploadAuditLogs(ctx context.Context) stream.StreamingClient {
	var clients []stream.StreamingClient

	for idx := range data.plugins {
		plugin := data.plugins[idx]
		logger := log.FromContext(ctx).WithValues("plugin", plugin.Name())

		if slices.Contains(plugin.LogsCapabilities(), logs.LogsCapability_RPC_TYPE_AUDIT_LOGS) {
			grpcClient, err := plugin.LogsClient().UploadAuditLogs(ctx)
			if err != nil {
				logger.Error(err, "failed to open streaming client")
				continue
			}
			rpc := &streamingRPC{
				hasAuditLogCapability: true,
				auditLogs:             grpcClient,
			}
			clients = append(clients, stream.NewStreamingClient(rpc))
		}
	}

	return stream.NewStreamingClientProxy(clients...)
}

type streamingRPC struct {
	hasServerLogCapability bool
	hasAuditLogCapability  bool

	serverLogs grpc.ClientStreamingClient[logs.UploadServerLogsRequest, logs.UploadServerLogsResult]
	auditLogs  grpc.ClientStreamingClient[logs.UploadAuditLogsRequest, logs.UploadAuditLogsResult]
}

func (s streamingRPC) UploadServerLogs(entries []*logs.UploadServerLogsRequest_Entry) error {
	if s.hasServerLogCapability {
		return s.serverLogs.Send(&logs.UploadServerLogsRequest{Entries: entries})
	}
	return errors.New("server logs capability not enabled")
}

func (s streamingRPC) UploadAuditLogs(entries []*logs.UploadAuditLogsRequest_Entry) error {
	if s.hasAuditLogCapability {
		return s.auditLogs.Send(&logs.UploadAuditLogsRequest{Entries: entries})
	}
	return errors.New("audit logs capability not enabled")
}

func (s streamingRPC) Close() error {
	return s.Close()
}
