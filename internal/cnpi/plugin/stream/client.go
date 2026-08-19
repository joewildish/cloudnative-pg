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

	"github.com/cloudnative-pg/cnpg-i/pkg/logs"
	"github.com/cloudnative-pg/machinery/pkg/log"
)

type StreamingRPC interface {
	UploadServerLogs(entries []*logs.UploadServerLogsRequest_Entry) error
	UploadAuditLogs(entries []*logs.UploadAuditLogsRequest_Entry) error
	Close() error
}

type StreamingClient interface {
	Start(ctx context.Context) error
	SendServerLog(log ServerLog) error
	SendAuditLog(log AuditLog) error
	Close(ctx context.Context)
}

type client struct {
	rpc StreamingRPC

	serverLogBuffer ServerLogBoundedBuffer
	serverLogMetric StreamingMetric

	auditLogBuffer AuditLogBoundedBuffer
	auditLogMetric StreamingMetric
}

func NewStreamingClient(rpc StreamingRPC) StreamingClient {
	return &client{
		rpc:             rpc,
		serverLogBuffer: NewServerLogBounderBuffer(10 * 1024 * 1024),
		serverLogMetric: &noopLogSenderMetric{},
		auditLogBuffer:  NewAuditLogBounderBuffer(10 * 1024 * 1024),
		auditLogMetric:  &noopLogSenderMetric{},
	}
}

func (c *client) Start(ctx context.Context) error {
	defer func() { _ = c.rpc.Close() }()
	send := func() error {
		if entries := toEntries(c.serverLogBuffer.Drain()); len(entries) > 0 {
			return c.rpc.UploadServerLogs(entries)
		}
		return nil
	}
	for {
		select {
		case <-c.serverLogBuffer.Wait():
			if err := send(); err != nil {
				return err
			}
		case <-ctx.Done():
			return send()
		}
	}
}

func (c *client) SendServerLog(log ServerLog) error {
	if c.serverLogBuffer.Push(log) {
		c.serverLogMetric.IncrementAccepted()
	} else {
		c.serverLogMetric.IncrementDropped()
	}
	return nil
}

func (c *client) SendAuditLog(log AuditLog) error {
	if c.auditLogBuffer.Push(log) {
		c.auditLogMetric.IncrementAccepted()
	} else {
		c.auditLogMetric.IncrementDropped()
	}
	return nil
}

func (c *client) Close(ctx context.Context) {
	if err := c.rpc.Close(); err != nil {
		log.FromContext(ctx).Error(err, "when closing streaming client")
	}
}

func toEntry(item ServerLog) *logs.UploadServerLogsRequest_Entry {
	return &logs.UploadServerLogsRequest_Entry{
		LogTime:              item.LogTime,
		UserName:             item.UserName,
		DatabaseName:         item.DatabaseName,
		ProcessId:            item.ProcessId,
		ConnectionFrom:       item.ConnectionFrom,
		SessionId:            item.SessionId,
		SessionLineNum:       item.SessionLineNum,
		CommandTag:           item.CommandTag,
		SessionStartTime:     item.SessionStartTime,
		VirtualTransactionId: item.VirtualTransactionId,
		TransactionId:        item.TransactionId,
		ErrorSeverity:        item.ErrorSeverity,
		SqlStateCode:         item.SqlStateCode,
		Message:              item.Message,
		Detail:               item.Detail,
		Hint:                 item.Hint,
		InternalQuery:        item.InternalQuery,
		InternalQueryPos:     item.InternalQueryPos,
		Context:              item.Context,
		Query:                item.Query,
		QueryPos:             item.QueryPos,
		Location:             item.Location,
		ApplicationName:      item.ApplicationName,
		BackendType:          item.BackendType,
		LeaderPid:            item.LeaderPid,
		QueryId:              item.QueryId,
	}
}

func toEntries(serverLogs []ServerLog) []*logs.UploadServerLogsRequest_Entry {
	entries := make([]*logs.UploadServerLogsRequest_Entry, len(serverLogs))
	for idx, serverLog := range serverLogs {
		entries[idx] = toEntry(serverLog)
	}
	return entries
}
