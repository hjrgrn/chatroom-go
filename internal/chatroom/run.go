package chatroom

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func NewChatRoom(dataDir string) (*ChatRoom, error) {
	cr := &ChatRoom{
		clients:       make(map[*Client]bool),
		join:          make(chan *Client),
		leave:         make(chan *Client),
		broadcast:     make(chan string),
		listUsers:     make(chan *Client),
		directMessage: make(chan DirectMessage),
		sessions:      make(map[string]*SessionInfo),
		messages:      make([]Message, 0),
		startTime:     time.Now(),
		dataDir:       dataDir,
	}
	// Restore from snapshot if available.
	if err := cr.loadSnapshot(); err != nil {
		fmt.Printf("Failed to load a shapshot: %v\n", err)
	}

	// Initalize WAL for new messages.
	if err := cr.initializePersistence(); err != nil {
		return nil, err
	}

	// Start background snapshot worker.
	go cr.periodicSnapshot()

	return cr, nil
}

func (cr *ChatRoom) periodicSnapshot() {
	// TODO: Make ticker configurable for testing purpose.
	// IDEA: It may be interesting to make ticker adaptable to traffic automatically.
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		cr.messageMu.Lock()
		messageCount := len(cr.messages)
		cr.messageMu.Unlock()

		if messageCount > 100 {
			if err := cr.createSnapshot(); err != nil {
				fmt.Printf("Snapshot failed: %v\n", err)
			}
		}
	}
}

func (cr *ChatRoom) Run() {
	// IDEA: shard the chat into multiple rooms, each with its own event loop.

	fmt.Println("ChatRoom heartbeat started..")
	// Time based, so not in the for loop.
	go cr.CleanupInactiveClients()

	for {
		select {
		case client := <-cr.join:
			cr.HandleJoin(client)

		case client := <-cr.leave:
			cr.HandleLeave(client)

		case message := <-cr.broadcast:
			cr.HandleBroadcast(message)

		case client := <-cr.listUsers:
			cr.sendUserList(client)

		case dm := <-cr.directMessage:
			cr.handleDirectMessage(dm)
		}
	}
}

func runServer() {
	chatRoom, err := NewChatRoom("./instance")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to launch the server: %v", err)
		return
	}
	defer chatRoom.shutdown()

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("Received shutdown signal.")
		chatRoom.shutdown()
		os.Exit(0)
	}()

	go chatRoom.Run()

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to launch the server: %v", err)
		return
	}
	defer listener.Close()

	fmt.Println("Server started on :9000")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, "Error accepting connection: %v", err)
			continue
		}
		fmt.Println("New connection accepted from: %v", conn.RemoteAddr())
		go handleClient(conn, chatRoom)
	}
}

func (cr *ChatRoom) shutdown() {
	fmt.Println("\nShutting down...")
	if err := cr.createSnapshot(); err != nil {
		fmt.Fprintln(os.Stderr, "Failed to create a snapshot: %v", err)
	}
	if cr.walFile != nil {
		cr.walFile.Close()
	}
	fmt.Println("See You Space Cowboy")
}
