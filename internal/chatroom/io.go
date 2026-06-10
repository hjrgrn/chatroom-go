package chatroom

import (
	"bufio"
	"fmt"
	"go/format"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"golang.org/x/tools/go/analysis/passes/ifaceassert"
)

func handleClient(conn net.Conn, chatroom *ChatRoom) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Panic in handleClient: %v\n", r)
			conn.Close()
		}
	}()

	// Set initial timeout for username entry.
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))

	reader := bufio.NewReader(conn)

	// Prompt for username or reconnection.
	conn.Write([]byte("Enter username (or reconnect:<token>:<username>):\n"))

	input, err := reader.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read username: %v\n", err)
		return
	}
	input = strings.TrimSpace(input)

	var username string
	var reconnectToken string
	var isReconnectiong bool

	// Parse reconnection attempt.
	if strings.HasPrefix(input, "reconnect:") {
		parts := strings.Split(input, ":")
		if len(parts) == 3 {
			username = parts[1]
			reconnectToken = parts[2]
			isReconnectiong = true
		} else {
			conn.Write([]byte("Invalid format. Use: \"reconnect:<token>:<username>\""))
			return
		}
	} else {
		username = input
	}

	// Generate guest if empty.
	if username == "" {
		username = fmt.Sprintf("Guest%d", rand.Intn(1000))
	}

	// Validate reconnect or check for duplicate.
	if isReconnectiong {
		if chatroom.validateReconnectToken(username, reconnectToken) {
			fmt.Printf("%s reconnected successfully.\n", username)
			conn.Write([]byte(fmt.Sprintf("Welcome back, %s!\n", username)))
		} else {
			conn.Write([]byte("Invalid token or session expired.\n"))
			return
		}
	} else {
		// Prevent duplicate logins.
		if chatroom.isUsernameConnected(username) {
			conn.Write([]byte(fmt.Sprintf("User %s is already connected. Use \"reconnect\" if you lost connection.\n", username)))
			return
		}

		// Create or retrieve user session.
		chatroom.sessionsMu.Lock()
		existingSession := chatroom.sessions[username]
		chatroom.sessionsMu.Unlock()

		// TODO: this should be in the critical section?
		if existingSession != nil {
			token := existingSession.ReconnectToken
			msg := fmt.Sprintf("Tip: save the token: %s\n", token)
			msg += fmt.Sprintf("To reconnect type:\nreconnect:%s:%s\n", username, token)
			conn.Write([]byte(msg))
		} else {
			session := chatroom.createSession(username)
			token := session.ReconnectToken
			msg := fmt.Sprintf("Tip: save this token:\n%s\n", token)
			msg += fmt.Sprintf("To reconnect type:\nreconnect:%s:%s\n")
			conn.Write([]byte(msg))
		}
	}

	// Create client object.
	client := &Client{
		conn:           conn,
		username:       username,
		outgoing:       make(chan string, 10), // Buffered.
		lastActive:     time.Now(),
		reconnectToken: reconnectToken,
		isSlowClient:   rand.Float64() < 0.1, // less then 10% chance for testing.
	}

	// Clear timeout for normal operations.
	conn.SetReadDeadline(time.Time{})

	// Notify chatroom.
	chatroom.join <- client

	// Send welcom message.
	welcomeMsg := buildWelcomeMessage(username)
	conn.Write([]byte(welcomeMsg))

	// Start Read/Write loops.
	go readMessages(client, chatroom)
	writeMessages(client) // Blocks until disconnect.

	// Update session on disconnect.
	chatroom.updateSessionActivity(username)
	chatroom.leave <- client
}

func buildWelcomeMessage(username string) string {
	msg := fmt.Sprintf("Welcome, %s!\n", username)
	msg += "Commands:\n"
	msg += "  /users - List all users\n"
	msg += "  /history [N] - Show last N messages\n"
	msg += "  /msg <user> <msg> - Private message\n"
	msg += "  /token - Show your reconnect token\n"
	msg += "  /stats - Show your stats\n"
	msg += "  /quit - Leave\n"
	return msg
}

func readMessages(client *Client, chatRoom *ChatRoom) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Panic in readMessagesfor %s: %v\n", client.username, r)
		}
	}()

	reader := bufio.NewReader(client.conn)

	for {
		// Set 5 minute idle timeout.
		client.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

		message, err := reader.ReadString('\n')
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				fmt.Printf("%s timed out.\n", client.username)
			} else {
				fmt.Printf("%s disconnected.\n", client.username)
			}
			return
		}

		client.markActive() // Update activity timestamp.

		message = strings.TrimSpace(message)
		if message == "" {
			continue
		}

		client.mu.Lock()
		client.messagesRecv++
		client.mu.Unlock()

		// Process commands VS regular messages.
		if strings.HasPrefix(message, "/") {
			handleCommand(client, chatRoom, message)
			continue
		}

		formatted := fmt.Sprintf("[%s]: %s\n", client.username, message)
		chatRoom.broadcast <- formatted
	}
}

func writeMessages(client *Client) {
	// TODO:
}

func handleCommand(client *Client, chatRoom *ChatRoom, command string) {}
