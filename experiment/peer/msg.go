package peer

import (
	"bytes"
)

//type Message struct {
//	Header  MessageHeader
//	Content []byte
//}
//
//type MessageHeader struct {
//	Version       uint16
//	ContentType   uint16
//	ContentLength uint32
//	ContentHash   id.Hash
//}

type Version uint16
type Variant uint16
type Data [] byte

type Message struct {
	Version Version
	Variant Variant
	Data	Data
}

const (
	Reserved0 = Variant(0)
	Reserved1 = Variant(1)
	Ping    = Variant(2)
	PingAck = Variant(3)
	Push    = Variant(4)
	PushAck = Variant(5)
	Pull    = Variant(6)
	PullAck = Variant(7)
	DirectMessage = Variant(8)
)

func (msg Message) Equal(other *Message) bool {
	return msg.Version == other.Version && msg.Variant == other.Variant && bytes.Equal(msg.Data, other.Data)
}