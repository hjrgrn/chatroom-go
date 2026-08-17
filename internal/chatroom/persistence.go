package chatroom

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (cr *ChatRoom) createSnapshot() error {
	// TODO:
	return nil
}

func (cr *ChatRoom) initializePersistence() error {
	if err := os.MkdirAll(cr.dataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	walPath := filepath.Join(cr.dataDir, "messages.wal")
	if err := cr.recoverFromWAL(walPath); err != nil {
		fmt.Fprintf(os.Stderr, "Recovery failed: %v\n", err)
	}

	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open wal: %w", err)
	}

	cr.walFile = file
	fmt.Printf("WAL initialized: %s\n", walPath)
	return nil
}

func (cr *ChatRoom) recoverFromWAL(walPath string) error {
	file, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("No WAL found (fresh start)")
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	recovered := 0
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var msg Message
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			fmt.Printf("Skipping corrupt line: %s\n", line)
			continue
		}
		cr.messages = append(cr.messages, msg)

		if msg.ID >= cr.nextMessageID {
			cr.nextMessageID = msg.ID + 1
		}
		recovered++
	}

	// TODO: maybe contribute this upstream
	if err := scanner.Err(); err != nil {
		log.Fatalf("Fatal error encountered during WAL recovery: %v", err)
	}

	fmt.Printf("Recovered %d messages\n", recovered)
	return nil
}

func (cr *ChatRoom) loadSnapshot() error {
	// TODO:
	return nil
}

func (cr *ChatRoom) persistMessage(msg Message) error {
	cr.walMu.Lock()
	defer cr.walMu.Unlock()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = cr.walFile.Write(append(data, '\n'))

	if err != nil {
		return err
	}

	return cr.walFile.Sync()
}
