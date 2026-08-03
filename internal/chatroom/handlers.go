package chatroom

import (
	"fmt"
	"os"
	"strconv"
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
	cr.messageMu.Lock()
	defer cr.messageMu.Unlock()

	length := len(cr.messages)
	start := max(length-count, 0)

	var historyMsgBuilder strings.Builder
	historyMsgBuilder.WriteString("Recent Messages:\n")

	for i := start; i < length; i++ {
		msg := cr.messages[i]
		historyMsgBuilder.WriteString("[")
		historyMsgBuilder.WriteString(msg.From)
		historyMsgBuilder.WriteString("]: ")
		historyMsgBuilder.WriteString(msg.Content)
		historyMsgBuilder.WriteString("\n")
	}
	historyMsg := historyMsgBuilder.String()

	select {
	case client.outgoing <- historyMsg:
	default:
	}

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
	cr.mu.Lock()
	defer cr.mu.Unlock()

	var listBuilder strings.Builder
	for c := range cr.clients {
		listBuilder.WriteString(" - ")
		listBuilder.WriteString(c.username)
		if c.isInactive(1 * time.Minute) {
			listBuilder.WriteString(" (idle)")
		}
		listBuilder.WriteString("\n")
	}

	listBuilder.WriteString("\nTotalMessages: ")
	listBuilder.WriteString(strconv.Itoa(cr.totalMessages))
	listBuilder.WriteString("\nUptime: ")
	listBuilder.WriteString(time.Since(cr.startTime).Round(time.Second).String())
	list := listBuilder.String()

	select {
	case client.outgoing <- list:
	default:
	}
}

func (cr *ChatRoom) handleDirectMessage(dm DirectMessage) {
	select {
	case dm.toClient.outgoing <- dm.message:
		dm.toClient.mu.Lock()
		dm.toClient.messagesSent++
		dm.toClient.mu.Unlock()
	default:
		fmt.Fprintf(os.Stderr, "Couldn't deliver DM to %s\n", dm.toClient.username)
	}

}

func (cr *ChatRoom) findClientByUsername(username string) *Client {
	cr.mu.Lock()
	defer cr.mu.Unlock()

	for client := range cr.clients {
		if client.username == username {
			return client
		}
	}
	return nil

}

func (c *Client) markActive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastActive = time.Now()
}

func (c *Client) isInactive(timeout time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Since(c.lastActive) > timeout
}
