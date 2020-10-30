package peer

import (
	"sync"

	"github.com/renproject/id"
)

// Force InMemTable to implement the Table interface.
var _ Table = &InMemTable{}

type Table interface {
	AddPeer(peerID id.Signatory, peerAddr string)
	DeletePeer(peerID id.Signatory)
	PeerAddress(peerID id.Signatory) (string, bool)
	PeerID(addr string) (id.Signatory, bool)
	All() []id.Signatory
}

type InMemTable struct {
	peersMu 	*sync.Mutex
	peers   	map[id.Signatory]string
	sigByAddrMu *sync.Mutex
	sigByAddr	map[string]id.Signatory
}

func NewInMemTable() *InMemTable {
	return &InMemTable{
		peersMu: new(sync.Mutex),
		peers:   map[id.Signatory]string{},
		sigByAddrMu: new(sync.Mutex),
		sigByAddr:	map[string]id.Signatory{},
	}
}

func (table *InMemTable) AddPeer(peerID id.Signatory, peerAddr string) {
	table.peersMu.Lock()
	table.sigByAddrMu.Lock()
	defer table.peersMu.Unlock()
	defer table.sigByAddrMu.Unlock()

	// TODO : Maybe refrain from overwriting the key if it already exists
	table.peers[peerID] = peerAddr
	table.sigByAddr[peerAddr] = peerID
}

func (table *InMemTable) DeletePeer(peerID id.Signatory) {
	table.peersMu.Lock()
	table.sigByAddrMu.Lock()
	defer table.peersMu.Unlock()
	defer table.sigByAddrMu.Unlock()
	if addr, ok := table.peers[peerID]; ok {
		delete(table.sigByAddr, addr)
	}
	delete(table.peers, peerID)
}

func (table *InMemTable) PeerAddress(peerID id.Signatory) (string, bool) {
	table.peersMu.Lock()
	defer table.peersMu.Unlock()

	val, ok := table.peers[peerID]
	return val, ok
}

func (table *InMemTable) PeerID(addr string) (id.Signatory, bool) {
	table.sigByAddrMu.Lock()
	defer table.sigByAddrMu.Unlock()

	val, ok := table.sigByAddr[addr]
	return val, ok
}

func (table *InMemTable) All() []id.Signatory {
	sigs := make([]id.Signatory, 0, len(table.peers))
	for k := range table.peers {
		sigs = append(sigs, k)
	}
	return sigs
}
