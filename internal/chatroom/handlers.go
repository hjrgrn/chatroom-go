package chatroom

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func (cr *ChatRoom) HandleJoin(client *Client) {
	// TODO:
}

func (cr *ChatRoom) HandleLeave(client *Client) {
	// TODO:
}

func (cr *ChatRoom) HandleBroadcast(message string) {
	// Parse message metadata.
	parts := strings.SplitN(message, ":", 2)
	from := "system"
	actualContent := message

	if len(parts) == 2 {
		from = strings.Trim(parts[0], "")
		actualContent = parts[1]
	}

	// Create persistent message record.
	cr.messageMu.Lock()
	msg := Message{
		ID:        cr.nextMessageID,
		From:      from,
		Content:   actualContent,
		Timestamp: time.Now(),
		Channel:   "global",
	}
	cr.nextMessageID++
	cr.messages = append(cr.messages, msg)
	cr.messageMu.Unlock()

	// Persist WAL.
	if err := cr.persistMessage(msg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to persist: %v\n", err)
		// Continue anyway - availability over consistency.
	}

	// Collect current clients
	cr.mu.Lock()
	clients := make([]*Client, 0, len(cr.clients))
	for client := range cr.clients {
		clients = append(clients, client)
	}
	cr.totalMessages++
	cr.mu.Unlock()

	fmt.Printf("Broadcasting to %d clients: %s", len(clients), message)

	// Fan-out to all clients.
	for _, client := range clients {
		select {
		case client.outgoing <- message:
			client.mu.Lock()
			client.messagesSent++
			client.mu.Unlock()
		default:
			fmt.Printf("Skipped %s (channel full)\n", client.username)
		}
	}
}

func (cr *ChatRoom) sendUserList(client *Client) {
	// TODO:
}

func (cr *ChatRoom) handleDirectMessage(message DirectMessage) {
	// TODO:
}

func (c *Client) markActive() {
	// TODO:
}
