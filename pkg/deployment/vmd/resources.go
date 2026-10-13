package vmd

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/hyscale-lab/aries/pkg/core"
	"github.com/hyscale-lab/aries/pkg/vmd/api"
)

// ResourceSource samples sandbox microVMs of one run occurrence through the
// daemon. It owns a separate connection, so sampling outlives the sandbox transport.
type ResourceSource struct {
	transport *Manager
	api       *api.Client
	runID     string
	taskID    string
}

// NewResourceSource scopes sampling to one run and task occurrence.
func NewResourceSource(options Options, runID, taskID string) (*ResourceSource, error) {
	if runID == "" || taskID == "" {
		return nil, errors.New("vmd resource source requires a run ID and task ID")
	}
	options.OnClose = nil
	transport, err := New(options)
	if err != nil {
		return nil, err
	}
	return &ResourceSource{transport: transport, api: transport.api, runID: runID, taskID: taskID}, nil
}

func (source *ResourceSource) Sample(ctx context.Context) ([]core.ResourceReading, error) {
	reply, err := source.api.Stats(ctx, &api.StatsRequest{Labels: map[string]string{"aries.managed": "true", api.LabelRun: source.runID, api.LabelTask: source.taskID}})
	if err != nil {
		return nil, fmt.Errorf("read vmd resource stats: %w", err)
	}
	readings := make([]core.ResourceReading, 0, len(reply.Sandboxes))
	for _, stats := range reply.Sandboxes {
		if stats.Labels[api.LabelRun] != source.runID || stats.Labels[api.LabelTask] != source.taskID {
			return nil, fmt.Errorf("vmd resource stats for %s have wrong ownership labels", stats.ID)
		}
		component := stats.Labels["aries.component"]
		if component != "sandbox" && component != "evaluation-sandbox" {
			continue
		}
		if stats.ObservedAt.IsZero() {
			return nil, fmt.Errorf("vmd resource stats for %s omit observation time", stats.ID)
		}
		readings = append(readings, core.ResourceReading{
			TaskID: source.taskID, Component: component, RuntimeID: stats.ID, RuntimeName: stats.ID,
			ObservedAt: stats.ObservedAt, CPUUsageNanoseconds: stats.CPUUsageNanoseconds,
			MemoryUsageBytes: stats.MemoryUsageBytes, MemoryLimitBytes: stats.MemoryLimitBytes,
		})
	}
	sort.Slice(readings, func(i, j int) bool {
		if readings[i].Component != readings[j].Component {
			return readings[i].Component < readings[j].Component
		}
		return readings[i].RuntimeID < readings[j].RuntimeID
	})
	return readings, nil
}

func (source *ResourceSource) Close() error {
	if source == nil || source.transport == nil {
		return nil
	}
	return source.transport.Close()
}
