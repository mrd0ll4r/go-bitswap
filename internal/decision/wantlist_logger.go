package decision

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	bsmsg "github.com/ipfs/go-bitswap/message"
	"github.com/joho/godotenv"
)

func (e *Engine) setUpWantlistLogging() error {
	err := godotenv.Load()
	if err != nil {
		log.Fatalw("unable to load .env file", "err", err)
		return err
	}

	wantlistLoggingMethod := os.Getenv("WANTLIST_LOGGING_METHOD")
	wantlistLoggingMethods := strings.Split(strings.TrimSpace(strings.ToLower(wantlistLoggingMethod)), ",")
	wantlistLoggingMethodsUnordered := make(map[string]struct{})
	for _, m := range wantlistLoggingMethods {
		m = strings.TrimSpace(m)
		wantlistLoggingMethodsUnordered[m] = struct{}{}
	}

	dupChans := make([]chan IncrementalWantListToLog, 0)

	for k := range wantlistLoggingMethodsUnordered {
		switch k {
		case "file":
			fmt.Println("logging wantlists to file")

			fileChan := make(chan IncrementalWantListToLog, 100)
			dupChans = append(dupChans, fileChan)

			go func() {
				w := NewRotateWriter("wantlist.json")
				ww := json.NewEncoder(w)

				for {
					iwl := <-fileChan
					err = ww.Encode(iwl)
					if err != nil {
						panic(err)
					}
				}
			}()
			break
		case "tcp":
			wantlistLoggingAddress := os.Getenv("WANTLIST_LOGGING_TCP_ADDRESS")
			listener, err := net.Listen("tcp", wantlistLoggingAddress)
			if err != nil {
				log.Fatalw("unable to bind for wantlist server", "err", err)
				return err
			}
			fmt.Printf("Listening on %s for wantlist subscriptions\n", listener.Addr())

			wantlistBufferSizeStr := os.Getenv("WANTLIST_LOGGING_BUFFER_SIZE")
			wantlistBufferSize := 100
			if wantlistBufferSizeStr != "" {
				s, err := strconv.Atoi(wantlistBufferSizeStr)
				if err != nil {
					log.Fatalw("unable to parse WANTLIST_LOGGING_BUFFER_SIZE", "err", err)
					return err
				}
				if s > 0 {
					wantlistBufferSize = s
				} else {
					log.Fatal("invalid WANTLIST_LOGGING_BUFFER_SIZE")
					return fmt.Errorf("invalid WANTLIST_LOGGING_BUFFER_SIZE")
				}
			}
			fmt.Printf("using wantlist buffer size = %d\n", wantlistBufferSize)

			tcpChan := make(chan IncrementalWantListToLog, 100)
			// We use this unbuffered channel to drop messages to TCP clients on backpressure.
			// We don't drop messages for the file logger, because that's important.
			// TODO maybe make this configurable
			tcpIntermediateChan := make(chan IncrementalWantListToLog, 0)
			go func() {
				for {
					msg := <-tcpIntermediateChan

					select {
					case tcpChan <- msg:
						break
					default:
						log.Errorw("dropping wantlists to TCP due to backpressure", "numEntries", len(msg.ReceivedEntries))
					}
				}
			}()
			dupChans = append(dupChans, tcpIntermediateChan)

			serveWantlistsTCP(tcpChan, listener, wantlistBufferSize)

			break
		default:
			return fmt.Errorf("unknown wantlist logging method %q", wantlistLoggingMethod)
		}
	}

	if len(dupChans) == 0 {
		fmt.Println("NO WANTLIST LOGGING SET UP!")
	}

	go func() {
		for {
			wantlist := <-e.wantListChan
			for _, c := range dupChans {
				c <- wantlist
			}
		}
	}()

	return nil
}

func serveWantlistsTCP(tcpChan chan IncrementalWantListToLog, listener net.Listener, wantlistBufferSize int) {
	clientChans := make([]struct {
		c      chan IncrementalWantListToLog
		closed chan struct{}
	}, 0)
	clientChansLock := &sync.RWMutex{}

	go func() {
		for {
			iwl := <-tcpChan
			needsCleanup := false
			clientChansLock.RLock()
			for _, v := range clientChans {
				select {
				case <-v.closed:
					needsCleanup = true
					continue
				default:
				}

				v.c <- iwl
			}
			clientChansLock.RUnlock()

			if needsCleanup {
				clientChansLock.Lock()
				for i, v := range clientChans {
					select {
					case <-v.closed:
						clientChans = append(clientChans[:i], clientChans[i+1:]...)
						close(v.c)
					default:
					}
				}
				clientChansLock.Unlock()
			}
		}
	}()

	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				log.Fatal("unable to accept wantlist logging connection: ", err)
			}
			fmt.Printf("new wantlist logging connection from %s\n", c.RemoteAddr())

			wlChan := make(chan IncrementalWantListToLog, 100)
			closedChan := make(chan struct{})

			clientChansLock.Lock()
			clientChans = append(clientChans, struct {
				c      chan IncrementalWantListToLog
				closed chan struct{}
			}{c: wlChan, closed: closedChan})
			clientChansLock.Unlock()

			go handleWantlistClient(wantlistBufferSize, wlChan, closedChan, c)
		}
	}()
}

func handleWantlistClient(wantlistBufferSize int, wlChan chan IncrementalWantListToLog, closedChan chan struct{}, c net.Conn) {
	buf := make([]IncrementalWantListToLog, 0, wantlistBufferSize)
	for {
		w := <-wlChan
		buf = append(buf, w)

		if len(buf) >= wantlistBufferSize {
			bbuf, err := json.Marshal(buf)
			if err != nil {
				log.Fatal("unable to encode JSON: ", err)
			}

			bufLen := len(bbuf)
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(bufLen))

			_, err = c.Write(lenBuf)
			if err != nil {
				if err == io.EOF {
					// Connection closed, probably?
					log.Warn("got EOF trying to write to ", c.RemoteAddr())
					close(closedChan)
					return
				}
				log.Warn("unable to write to ", c.RemoteAddr(), err)
				close(closedChan)
				return
			}

			_, err = c.Write(bbuf)
			if err != nil {
				if err == io.EOF {
					// Connection closed, probably?
					log.Warn("got EOF trying to write to ", c.RemoteAddr())
					close(closedChan)
					return
				}
				log.Warn("unable to write to ", c.RemoteAddr(), err)
				close(closedChan)
				return
			}

			buf = buf[:0]
		}
	}
}

type IncrementalWantListToLog struct {
	Timestamp       time.Time     `json:"timestamp"`
	Peer            string        `json:"peer"`
	ReceivedEntries []bsmsg.Entry `json:"received_entries"`
	FullWantList    bool          `json:"full_want_list"`
	// PeerConnected is set to true if we received a PeerConnected method call
	// with this peer.
	PeerConnected bool `json:"peer_connected"`
	// PeerDisconnected is set to true if we received a PeerDisconnected method
	// call with this peer.
	PeerDisconnected bool `json:"peer_disconnected"`
	// ConnectEventPeerFound is only valid if either PeerConnected or
	// PeerDisconnected are set.
	// It tracks whether we already had a ledger entry for the peer.
	ConnectEventPeerFound bool `json:"connect_event_peer_found"`
}

type RotateWriter struct {
	lock         sync.Mutex
	filename     string // should be set to the actual filename
	fp           *os.File
	lastRotation time.Time
}

func (w *RotateWriter) Close() error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.fp != nil {
		return w.fp.Close()
	}

	return nil
}

// Make a new RotateWriter. Return nil if error occurs during setup.
func NewRotateWriter(filename string) *RotateWriter {
	w := &RotateWriter{filename: filename,
		lastRotation: time.Unix(0, 0)}
	return w
}

// Write satisfies the io.Writer interface.
func (w *RotateWriter) Write(output []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	// Rotating in here guarantees that we actually want to write to it.
	// Otherwise we'll end up with tons of empty files.
	if time.Since(w.lastRotation) > time.Hour {
		err := w.rotate()
		if err != nil {
			return 0, err
		}
		w.lastRotation = time.Now()
	}

	return w.fp.Write(output)
}

// Perform the actual act of rotating and reopening file.
func (w *RotateWriter) rotate() (err error) {
	// Close existing file if open
	if w.fp != nil {
		err = w.fp.Close()
		w.fp = nil
		if err != nil {
			return
		}
	}
	// Rename dest file if it already exists
	_, err = os.Stat(w.filename)
	if err == nil {
		err = os.Rename(w.filename, w.filename+"."+time.Now().Format(time.RFC3339))
		if err != nil {
			return
		}
	}

	// Create a file.
	w.fp, err = os.Create(w.filename)
	return
}
