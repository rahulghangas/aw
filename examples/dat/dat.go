package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/renproject/aw/experiment/handshake"
	"github.com/renproject/aw/experiment/peer"
	"github.com/renproject/id"
)

func CLI(p *peer.Peer) {
	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("error reading user input: %v\n", err)
			continue
		}

		trimmedInput := strings.TrimSpace(input)
		switch string(trimmedInput[0]) {
		case ">":
			if err := handleInstruction(p, trimmedInput[1:]); err != nil {
				fmt.Printf("%v\n", err)
			}
		case "@":
			if err := handleDirectMessage(p, trimmedInput[1:]); err != nil {
				fmt.Printf("%v\n", err)
			}
		case "#":
			handleBroadcastToRoom(p, trimmedInput[1:])
		// case "*":
		// 	if err := handleGlobalBroadcast(p, trimmedInput[1:]); err != nil {
		// 		fmt.Printf("%v\n", err)
		// 	}
		default:
			fmt.Println("err - Invalid msg Prefix")
		}
	}
}

func handleInstruction(p *peer.Peer, data string) error {
	trimmedData := strings.TrimLeft(data, " ")
	splitIndex := strings.Index(trimmedData, " ")

	if splitIndex == -1 {
		switch trimmedData {
		case "ping":
			return fmt.Errorf("err - ping is unimplemented")
		case "info":
			fmt.Printf("My ID: %v\n", p.ID().String())
			fmt.Printf("My Public Key: %x %x\n", p.Options().PrivKey.PubKey().X, p.Options().PrivKey.PubKey().Y)
			fmt.Printf("Currently added peers: \n%v\n", p.Table())
			return nil
		default:
			return fmt.Errorf("err - invalid instruction")
		}
	}

	switch trimmedData[:splitIndex] {
	case "leave":
		return fmt.Errorf("err - leave is unimplemented")
	case "add":
		args := strings.Fields(data[splitIndex:])
		if len(args) != 2 {
			return fmt.Errorf("err - invalid arguments to addition instruction")
		}

		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(args[0], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		p.Table().AddPeer(sig, args[1])
	case "del":
		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(data[splitIndex:], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		p.Table().DeletePeer(sig)
	case "get":
		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(data[splitIndex:], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		address, ok := p.Table().PeerAddress(sig)
		if !ok {
			return fmt.Errorf("err - unknown peer id: %v", id.Signatory(sig).String())
		}
		fmt.Printf("Address associated with peer is: %v\n", address)
	default:
		return fmt.Errorf("err - invalid instruction")
	}
	return nil
}

func handleDirectMessage(p *peer.Peer, data string) error {
	trimmedData := strings.TrimLeft(data, " ")
	splitIndex := strings.Index(trimmedData, " ")

	if splitIndex == -1 {
		return fmt.Errorf("err - empty message")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := [32]byte{}
	decodedBytes, err := base64.RawURLEncoding.DecodeString(data[:splitIndex])
	if err != nil {
		return fmt.Errorf("err - string id could not be decoded")
	}

	copy(sig[:], decodedBytes)
	msg := peer.Message{Variant: peer.DirectMessage,Data: []byte(strings.TrimLeft(data[splitIndex:], " "))}
	if err := p.Send(ctx, sig, msg); err != nil {
		fmt.Printf("%v", err)
		return fmt.Errorf("err - message could not be sent")
	}

	return nil
}

func handleBroadcastToRoom(p *peer.Peer, data string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Gossip(ctx, id.Hash{}, []byte(data))
}

// func handleGlobalBroadcast(p *peer.Peer, data string) error {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	for sig, _ := range p.Options() {
// 		p.Send(ctx, sig, data)
// 	}
// }

func main() {

	args := os.Args
	num, _ := strconv.Atoi(args[1])
	port := uint16(num)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := peer.New(
		peer.DefaultOptions().WithPort(port),
		peer.NewInMemTable())
	*p.Options() = p.Options().WithHandshake(handshake.ECIESClientHandshake(p.Options().PrivKey, rand.New(rand.NewSource(time.Now().UnixNano()))),
		handshake.ECIESServerHandshake(p.Options().PrivKey, rand.New(rand.NewSource(time.Now().UnixNano()))))

	go p.Run(ctx)

	CLI(p)
}
