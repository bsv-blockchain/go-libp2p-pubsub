package pubsub

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

// A peer keeps its subscriptions after our writer to it is gone (while it is
// respawned, or after the dead-peer backoff gave up), as long as it stays
// connected. These tests put such peers straight into a topic: subscribed,
// with no writer and no router state.
func addWriterlessPeers(t *testing.T, ps *PubSub, topic string, n int) []peer.ID {
	t.Helper()

	pids := make([]peer.ID, n)
	for i := range pids {
		pids[i] = test.RandPeerIDFatal(t)
	}

	onProcessLoop(ps, func() struct{} {
		tmap, ok := ps.topics[topic]
		if !ok {
			tmap = make(map[peer.ID]peerTopicState)
			ps.topics[topic] = tmap
		}
		for _, pid := range pids {
			tmap[pid] = peerTopicState{}
		}
		return struct{}{}
	})

	return pids
}

func TestFloodsubEnoughPeersIgnoresPeersWithoutWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "writerless"

	hosts := getDefaultHosts(t, 1)
	ps := getPubsub(ctx, hosts[0])
	if _, err := ps.Subscribe(topic); err != nil {
		t.Fatal(err)
	}

	addWriterlessPeers(t, ps, topic, FloodSubTopicSearchSize)

	if enoughPeers(ps, topic, 1) {
		t.Fatal("a subscribed peer without a writer counts towards MinTopicSize")
	}
	if enoughPeers(ps, topic, 0) {
		t.Fatal("subscribed peers without a writer satisfy discovery's default search size")
	}
}

func TestGossipsubIgnoresGraftFromPeerWithoutWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "writerless"

	hosts := getDefaultHosts(t, 1)
	ps := getGossipsub(ctx, hosts[0])
	if _, err := ps.Subscribe(topic); err != nil {
		t.Fatal(err)
	}

	pid := addWriterlessPeers(t, ps, topic, 1)[0]
	gs := ps.rt.(*GossipSubRouter)

	prune := onProcessLoop(ps, func() []*pb.ControlPrune {
		return gs.handleGraft(pid, &pb.ControlMessage{Graft: []*pb.ControlGraft{{TopicID: &[]string{topic}[0]}}})
	})
	if len(prune) != 0 {
		t.Fatalf("PRUNE queued to a peer without a writer: %v", prune)
	}

	inMesh := onProcessLoop(ps, func() bool { _, ok := gs.mesh[topic][pid]; return ok })
	if inMesh {
		t.Fatal("GRAFT put a peer without a writer in the mesh")
	}
	if enoughPeers(ps, topic, 1) {
		t.Fatal("a grafted peer without a writer counts towards MinTopicSize")
	}
}

func TestRandomsubPublishSkipsPeersWithoutWriter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const topic = "writerless"
	const messages = 50

	hosts := getDefaultHosts(t, 2)
	psubs := make([]*PubSub, len(hosts))
	for i, h := range hosts {
		ps, err := NewRandomSub(ctx, h, len(hosts))
		if err != nil {
			t.Fatal(err)
		}
		psubs[i] = ps
	}

	tp, err := psubs[0].Join(topic)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := psubs[1].Subscribe(topic)
	if err != nil {
		t.Fatal(err)
	}

	connect(t, hosts[0], hosts[1])
	waitUntil(t, "host 0 never saw host 1's subscription", func() bool {
		return slices.Contains(psubs[0].ListPeers(topic), hosts[1].ID())
	})

	// More peers than RandomSubD, so the publisher samples; only host 1 can
	// receive the message.
	addWriterlessPeers(t, psubs[0], topic, RandomSubD)

	for i := 0; i < messages; i++ {
		payload := fmt.Sprintf("msg-%d", i)
		if err := tp.Publish(ctx, []byte(payload)); err != nil {
			t.Fatal(err)
		}

		recvCtx, recvCancel := context.WithTimeout(ctx, 2*time.Second)
		msg, err := sub.Next(recvCtx)
		recvCancel()
		if err != nil {
			t.Fatalf("message %d never reached the only peer with a writer: %v", i, err)
		}
		if string(msg.Data) != payload {
			t.Fatalf("message %d: got %q", i, msg.Data)
		}
	}
}
