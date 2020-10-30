package peer

import (
	"context"
	"fmt"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

func DefaultDidReceiveMessage(p *Peer, remote id.Signatory, msg Message) {
	switch msg.Variant {
	case Ping:
		DefaultDidReceivePing(p, remote, msg)
	case PingAck:
		DefaultDidReceivePingAck(p, remote, msg)
	case Push:
		DefaultDidReceivePush(p, remote, msg)
	case PushAck:
		DefaultDidReceivePushAck(p, remote, msg)
	case Pull:
		DefaultDidReceivePull(p, remote, msg)
	case PullAck:
		DefaultDidReceivePullAck(p, remote, msg)
	case DirectMessage:
		fmt.Printf("received message from %v: %v\n", remote, string(msg.Data))
	default:
		p.opts.Logger.Error("listener", zap.String("message type", "invalid"))
	}
}

func DefaultDidReceivePing(p *Peer, remote id.Signatory, msg Message) {
	fmt.Println("Ping")
}

func DefaultDidReceivePingAck(p *Peer, remote id.Signatory, msg Message) {
	fmt.Println("PingAck")
}

func DefaultDidReceivePush(p *Peer, remote id.Signatory, msg Message) {
	fmt.Println("Push message received")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	response := Message{
		Variant: PushAck,
		Data:    nil,
	}
	if err := p.Send(ctx, remote, response); err != nil {
		p.opts.Logger.Error("gossip", zap.NamedError("pushAck", err))
		return
	}

	hash := [32]byte{}
	copy(hash[:], msg.Data[:])

	if _, ok := p.contentTable.Content(hash, uint8(Push)); !ok {
		response = Message{
			Variant: Pull,
			Data:    hash[:],
		}

		p.contentTable.Insert(hash, uint8(Pull), nil)
		if err := p.Send(ctx, remote, response); err != nil {
			p.opts.Logger.Error("gossip", zap.NamedError("pull", err))
			p.contentTable.Delete(hash, uint8(Pull))
			return
		}

	}
}

func DefaultDidReceivePushAck(p *Peer, remote id.Signatory, msg Message) {
	fmt.Println("PushAck received")
}

func DefaultDidReceivePull(p *Peer, remote id.Signatory, msg Message) {
	fmt.Println("Pull request incoming")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hash := [32]byte{}
	copy(hash[:], msg.Data[:])
	if data, ok := p.contentTable.Content(hash, uint8(Push)); ok {
		response := Message{
			Variant: PullAck,
			Data:    data,
		}

		if err := p.Send(ctx, remote, response); err != nil {
			p.opts.Logger.Error("gossip", zap.NamedError("pull", err))
		}
		return
	}

	p.opts.Logger.Error("gossip", zap.String("pull request", "data not present"))

}

func DefaultDidReceivePullAck(p *Peer, remote id.Signatory, msg Message) {
	hash := id.NewHash(msg.Data)

	if _, ok := p.contentTable.Content(hash, uint8(Pull)); !ok {
		p.opts.Logger.Error("gossip", zap.String("pull ack", "illegal data received"))
		return
	}
	p.contentTable.Delete(hash, uint8(Pull))

	fmt.Printf("message forwarded from %v: %v\n", remote, string(msg.Data))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Gossip(ctx, id.Hash{}, msg.Data)
}