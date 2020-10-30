package peer

import (
	"context"
	"errors"
	"fmt"
	"github.com/renproject/aw/dht"
	"net"
	"time"

	"github.com/renproject/aw/experiment/channel"
	"github.com/renproject/aw/experiment/policy"
	"github.com/renproject/aw/experiment/tcp"
	"github.com/renproject/id"
	"github.com/renproject/surge"
	"go.uber.org/zap"
)

var (
	// GlobalSubnet is a reserved subnet identifier that is used to reference
	// the entire peer-to-peer network.
	GlobalSubnet = id.Hash{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
)

var (
	ErrPeerNotFound = errors.New("peer not found")
)

type Peer struct {
	opts  Options
	pool  *channel.Pool
	table Table
	contentTable dht.ContentResolver
}

func New(opts Options, table Table) *Peer {
	return &Peer{
		opts:  opts,
		pool:  channel.NewPool(channel.DefaultPoolOptions()),
		table: table,
		contentTable: dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil),
	}
}

func (p *Peer) ID() id.Signatory {
	return p.opts.PrivKey.Signatory()
}

func (p *Peer) Table() Table {
	return p.table
}

func (p *Peer) Options() *Options {
	return &p.opts
}

func (p *Peer) MessageLogBook() dht.ContentResolver {
	return p.contentTable
}

func (p *Peer) Pool() *channel.Pool {
	return p.pool
}

// Ping initiates a round of peer discovery in the network. The peer will
// attempt to gossip its identity throughout the network, and discover the
// identity of other remote peers in the network. It will continue doing so
// until the context is done.
func (p *Peer) Ping(ctx context.Context) error {
	panic("unimplemented")
}

func (p *Peer) Send(ctx context.Context, remote id.Signatory, msg Message) error {
	remoteAddr, ok := p.table.PeerAddress(remote)
	if !ok {
		return ErrPeerNotFound
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ch, ok := p.pool.Channel(remote)
	if !ok {
		go p.dial(context.Background(), remoteAddr, &msg)
		return nil
	}

	data, err := surge.ToBinary(msg)
	if err != nil {
		return fmt.Errorf("marshal: %v", err)
	}

	if _, err := ch.Write(data); err != nil {
		return fmt.Errorf("write: %v", err)
	}

	return nil
}

func (p *Peer) Gossip(ctx context.Context, subnet id.Hash, data []byte) {
	sig := id.Signatory(subnet)
	hash := id.NewHash(data)
	p.contentTable.Insert(hash, uint8(Push), data)
	msg := Message{Variant: Push, Data: hash[:]}

	if _, ok := p.table.PeerAddress(sig); ok {
		if err := p.Send(ctx, sig, msg); err != nil {
			p.opts.Logger.Error("gossip", zap.Error(err))
		}
		return
	}

	p.broadcast(ctx, subnet, msg)
}

func (p *Peer) broadcast(ctx context.Context, subnet id.Hash, msg Message) {
	for _, sig := range p.table.All() {
		if err := p.Send(ctx, sig, msg); err != nil {
			p.opts.Logger.Error("gossip", zap.Error(err))
		}
	}
}

// Run the peer until the context is done. If running encounters an error, or
// panics, it will automatically recover and continue until the context is done.
func (p *Peer) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			p.run(ctx)
		}
	}
}

func (p *Peer) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			p.opts.Logger.DPanic("recover", zap.Error(fmt.Errorf("%v", r)))
		}
	}()

	self := id.NewSignatory(p.opts.PrivKey.PubKey())

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	open := p.pool.HighestPeerWinsHandshake(self, p.opts.ServerHandshake)

	tcp.Listen(
		ctx,
		fmt.Sprintf("%v:%v", p.opts.Bind, p.opts.Port),
		func(conn net.Conn) {
			_, dec, remote, err := open(conn, p.opts.Encoder, p.opts.Decoder)
			if err != nil {
				p.opts.Logger.Error("handshake", zap.String("remote", conn.RemoteAddr().String()), zap.Error(err))
				return
			}

			if _, ok := p.table.PeerAddress(remote); !ok {
				p.table.AddPeer(remote, conn.RemoteAddr().String())
			}

			buf := [1024 * 1024]byte{}
			for {
				n, err := dec(conn, buf[:])
				if err != nil {
					p.opts.Logger.Error("decode", zap.String("remote", conn.RemoteAddr().String()), zap.ByteString("buf", buf[:n]), zap.Error(err))
					return
				}
				data := buf[:n]
				msg := Message{}
				if err := surge.FromBinary(&msg, data); err != nil {
					p.opts.Logger.Error("unmarshal", zap.String("remote", conn.RemoteAddr().String()), zap.Error(err))
					return
				}

				p.opts.Callbacks.DidReceiveMessage(p, remote, msg)
			}

		},
		func(err error) {
			p.opts.Logger.Error("listen", zap.Error(err))
		},
		ReuseConn(p))
}

func (p *Peer) dial(ctx context.Context, remoteAddr string, msg *Message) error {
	var closureErr error
	self := p.opts.PrivKey.Signatory()
	h := p.pool.HighestPeerWinsHandshake(
		self,
		p.opts.ClientHandshake,
	)
	err := tcp.Dial(
		ctx,
		remoteAddr,
		func(conn net.Conn) {
			enc, dec, remote, err := h(conn, p.opts.Encoder, p.opts.Decoder)
			if err != nil {
				closureErr = err
				return
			}
			data, err := surge.ToBinary(msg)
			if err != nil {
				closureErr = err
				return
			}

			_, closureErr = enc(conn, data)

			buf := [1024 * 1024]byte{}
			for {
				n, err := dec(conn, buf[:])
				if err != nil {
					p.opts.Logger.Error("decode", zap.String("remote", conn.RemoteAddr().String()), zap.ByteString("buf", buf[:n]), zap.Error(err))
					return
				}
				data := buf[:n]
				msg := Message{}
				if err := surge.FromBinary(&msg, data); err != nil {
					p.opts.Logger.Error("unmarshal", zap.String("remote", conn.RemoteAddr().String()), zap.Error(err))
					return
				}

				p.opts.Callbacks.DidReceiveMessage(p, remote, msg)
			}
		},
		func(err error) {
			p.opts.Logger.Error("send", zap.String("remote", remoteAddr), zap.Error(err))
		},
		policy.ConstantTimeout(time.Second)) // TODO: This should be configurable.
	if err != nil {
		return fmt.Errorf("dialing %v: %v", remoteAddr, err)
	}

	return closureErr
}

var ErrConnectionToAddrExists = errors.New("connection to remote IP address already exists")

func ReuseConn(p *Peer) policy.Allow {
	return func(conn net.Conn) (error, policy.Cleanup) {
		remoteAddr := ""
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			remoteAddr = tcpAddr.IP.String()
		} else {
			remoteAddr = conn.RemoteAddr().String()
		}

		sig, ok := p.Table().PeerID(remoteAddr)
		if ok {
			return ErrConnectionToAddrExists, nil
		}
		return nil, func() {
			p.Table().DeletePeer(sig)
			p.Pool().Close(sig)
		}
	}
}
