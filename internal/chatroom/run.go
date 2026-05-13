package chatroom

import (
	"fmt"
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
	// TODO: make ticker configurable for testing purpose.
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
	fmt.Println("ChatRoom heartbeat started..")
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
