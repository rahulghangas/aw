package peer

import (
	"bytes"
	"context"
	"fmt"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
)

type GossipFunc func(ctx context.Context, subnet id.Hash, contentID []byte) error

// Gossiper returns a new set of callback functions and a gossip function that can be used to spread information throughout a network.
func Gossiper(logger *zap.Logger, t *transport.Transport, contentResolver dht.ContentResolver, addressTable Table, next Callbacks) (Callbacks, GossipFunc) {

	gossip := func(ctx context.Context, subnet id.Hash, contentID []byte) error {
		msg := wire.Msg{Version: wire.MsgVersion1, Type: wire.MsgTypePush, Data: contentID}

		var chainedError error = nil
		var receivers []id.Signatory
		if bytes.Compare(subnet[:], GlobalSubnet[:]) == 0 {
			receivers = addressTable.All()
		} else {
			receivers = addressTable.Subnet(subnet)
		}
		
		for _, sig := range receivers {
			addr, ok := addressTable.PeerAddress(sig)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				continue
			}
			if err := t.Send(ctx, sig, addr, msg); err != nil {
				if chainedError == nil {
					chainedError = fmt.Errorf("%v, gossiping to peer %v : %v", chainedError, sig, err)
				} else {
					chainedError = fmt.Errorf("gossiping to peer %v : %v", sig, err)
				}
			}
		}
		return chainedError
	}
	didReceivePush := func(from id.Signatory, msg wire.Msg) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		addr, ok := addressTable.PeerAddress(from)
		if !ok {
			logger.Error("gossip", zap.String("table", "peer not found"))
			return
		}

		hash := [32]byte{}
		copy(hash[:], msg.Data[:])

		if _, ok := contentResolver.Content(hash); !ok {
			response := wire.Msg{
				Version: wire.MsgVersion1,
				Type: wire.MsgTypePull,
				Data: hash[:],
			}

			contentResolver.Insert(hash, nil)
			if err := t.Send(ctx, from, addr, response); err != nil {
				contentResolver.Delete(hash)
				logger.Error("gossip", zap.NamedError("pull", err))
				return
			}

		}
	}
	didReceivePull := func(from id.Signatory, msg wire.Msg) {
		hash := [32]byte{}
		copy(hash[:], msg.Data[:])
		if data, ok := contentResolver.Content(hash); ok {
			response := wire.Msg{
				Version: wire.MsgVersion1,
				Type: wire.MsgTypeSync,
				Data: data,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			addr, ok := addressTable.PeerAddress(from)
			if !ok {
				logger.Error("gossip", zap.String("table", "peer not found"))
				return
			}
			if err := t.Send(ctx, from, addr, response); err != nil {
				logger.Error("gossip", zap.NamedError("pull", err))
			}
			return
		}

		logger.Error("gossip", zap.String("pull request", "data not present"))
	}
	didReceiveSync := func(from id.Signatory, msg wire.Msg) {
		hash := id.NewHash(msg.Data)
		content, ok := contentResolver.Content(dht.ContentID(hash))
		if !ok {
			logger.Error("gossip", zap.String("sync", "unknown id in sync message"))
			return
		}
		if content != nil {
			// TODO : Add debugging message for logger
			return
		}
		contentResolver.Insert(dht.ContentID(hash), msg.Data)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if err := gossip(ctx, id.Hash{}, hash[:]); err != nil {
			logger.Error("Gossiping synced message" , zap.Error(err))
		}
	}

	return Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				switch msg.Type {
				case wire.MsgTypePush:
					didReceivePush(from, msg)
				case wire.MsgTypePull:
					didReceivePull(from, msg)
				case wire.MsgTypeSync:
					didReceiveSync(from, msg)
				case wire.MsgTypeDirect:
					fmt.Println(string(msg.Data))
				}

				//if next.DidReceiveMessage != nil {
				//	next.DidReceiveMessage(from, msg)
				//}
			},
		},
		gossip
}