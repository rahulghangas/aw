package handshake_test

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/rahulghangas/aw/codec"
	"github.com/rahulghangas/aw/handshake"
	"github.com/rahulghangas/aw/policy"
	"github.com/rahulghangas/aw/tcp"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handshake", func() {

	run := func(ctx context.Context, dialAfter, listenAfter time.Duration, shouldListen bool, dialRetry, dialSuccess chan bool) {
		if shouldListen {
			go func() {
				time.Sleep(0)
				<-time.After(listenAfter)

				privKey := id.NewPrivKey()
				h := handshake.ECIES(privKey)

				tcp.Listen(ctx,
					"127.0.0.1:3334",
					func(conn net.Conn) {
						h(conn,
							codec.PlainEncoder,
							codec.PlainDecoder,
						)
					},
					nil,
					nil,
				)
			}()
		}

		go func() {
			time.Sleep(0)
			<-time.After(dialAfter)

			retrySignalOnce := sync.Once{}
			privKey := id.NewPrivKey()
			h := handshake.ECIES(privKey)

			tcp.Dial(ctx,
				"127.0.0.1:3334",
				func(conn net.Conn) {
					_, _, _, err := h(conn,
						codec.PlainEncoder,
						codec.PlainDecoder)
					if err == nil {
						dialSuccess <- true
					}
				},
				func() func(error) {
					return func(error) {
						retrySignalOnce.Do(func() {
							dialRetry <- true
						})
					}
				}(),
				policy.ConstantTimeout(50*time.Millisecond),
			)
		}()
	}

	Context("connecting a client to a server", func() {
		When("the server is online", func() {
			It("should connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = nil, make(chan bool)
				run(ctx, 500*time.Millisecond, 0, true, dialRetry, dialSuccess)
				Expect(<-dialSuccess).Should(BeTrue())
			})
		})

		When("the server is offline", func() {
			It("should retry", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
				run(ctx, 0, 500*time.Millisecond, false, dialRetry, dialSuccess)
				Expect(<-dialRetry).Should(BeTrue())
			})

			It("if the server comes online should eventually connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
				run(ctx, 0, 500*time.Millisecond, true, dialRetry, dialSuccess)
				Expect(<-dialRetry).Should(BeTrue())
				Expect(<-dialSuccess).Should(BeTrue())
			})
		})
	})
})
