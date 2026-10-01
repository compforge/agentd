package k8s

import (
	"context"
	"fmt"

	"github.com/compforge/agentd/agentd/internal/worker/cluster"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// PodInformer maintains a local cache of managed Agentlet Pods. Event
// callbacks feed cluster diagnostic hooks and signal cache changes. Consumers
// rebuild their view from ListAgentletPods so bursts can be safely coalesced.
type PodInformer struct {
	podHooks cluster.PodHooks
	informer cache.SharedIndexInformer
}

func (c *Client) NewAgentletPodInformer() *PodInformer {
	informer := coreinformers.NewFilteredPodInformer(
		c.client,
		c.namespace,
		0,
		cache.Indexers{},
		func(options *metav1.ListOptions) {
			options.LabelSelector = c.labelSelector
		},
	)
	return &PodInformer{informer: informer, podHooks: c.podHooks}
}

// Start registers a non-blocking cache-change callback and waits for the
// initial LIST to populate the cache. The informer continues running until ctx
// is cancelled.
func (i *PodInformer) Start(ctx context.Context, notify func()) error {
	if notify == nil {
		return fmt.Errorf("start Agentlet Pod informer: notify callback is required")
	}
	observe := func(obj any) {
		if pod, ok := obj.(*corev1.Pod); ok && i.podHooks != nil {
			i.podHooks.OnPodUpdated(ctx, pod)
		}
		notify()
	}
	if _, err := i.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    observe,
		UpdateFunc: func(_ any, obj any) { observe(obj) },
		DeleteFunc: func(any) { notify() },
	}); err != nil {
		return fmt.Errorf("register Agentlet Pod informer handler: %w", err)
	}
	go i.informer.Run(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), i.informer.HasSynced) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("sync Agentlet Pod informer cache")
	}
	return nil
}

func (i *PodInformer) ListAgentletPods(ctx context.Context) ([]PodSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	items := i.informer.GetStore().List()
	snapshots := make([]PodSnapshot, 0, len(items))
	for _, item := range items {
		pod, ok := item.(*corev1.Pod)
		if !ok {
			continue
		}
		snapshots = append(snapshots, snapshotFromPod(pod))
	}
	sortPodSnapshots(snapshots)
	return snapshots, nil
}
