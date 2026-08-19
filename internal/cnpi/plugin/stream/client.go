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
	Send(entries []*logs.SendLogsRequest_Entry) error
	Close() error
}

type StreamingClient interface {
	Start(ctx context.Context) error
	Send(log ServerLog) error
	Close(ctx context.Context)
}

type client struct {
	rpc    StreamingRPC
	buffer ServerLogBoundedBuffer
	metric StreamingMetric
}

func NewStreamingClient(rpc StreamingRPC) StreamingClient {
	return &client{
		rpc:    rpc,
		buffer: NewServerLogBounderBuffer(10 * 1024 * 1024),
		metric: &noopLogSenderMetric{},
	}
}

func (s *client) Start(ctx context.Context) error {
	defer func() { _ = s.rpc.Close() }()
	send := func() error {
		if entries := toEntries(s.buffer.Drain()); len(entries) > 0 {
			return s.rpc.Send(entries)
		}
		return nil
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

func (s *client) Send(log ServerLog) error {
	if s.buffer.Push(log) {
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

func toEntries(serverLogs []ServerLog) []*logs.SendLogsRequest_Entry {
	entries := make([]*logs.SendLogsRequest_Entry, len(serverLogs))
	for n, item := range serverLogs {
		entries[n] = &logs.SendLogsRequest_Entry{
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
	return entries
}
