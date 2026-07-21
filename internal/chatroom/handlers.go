package chatroom

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func (cr *ChatRoom) HandleJoin(client *Client) {
	cr.mu.Lock()
	cr.clients[client] = true
	cr.mu.Unlock()

	client.markActive()

	fmt.Printf("%s joined (total: %d)\n", client.username, len(cr.clients))

	cr.sendHistory(client, 10)

	cr.HandleBroadcast(fmt.Sprintf("*** %s joined the chat ***\n", client.username))
}

func (cr *ChatRoom) sendHistory(client *Client, count int) {
	// TODO:
}

func (cr *ChatRoom) HandleLeave(client *Client) {
	cr.mu.Lock()
	if !cr.clients[client] {
		cr.mu.Unlock()
		return
	}
	delete(cr.clients, client)
	cr.mu.Unlock()

	fmt.Printf("%s left (total: %d)\n", client.username, len(cr.clients))

	// Close channel safely
	select {
	case <-client.outgoing:
	default:
		close(client.outgoing)
	}

	cr.HandleBroadcast(fmt.Sprintf("*** %s left the chat ***\n", client.username))
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
