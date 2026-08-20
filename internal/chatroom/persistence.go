package chatroom

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
)

func (cr *ChatRoom) createSnapshot() error {
	snapshotPath := cr.getSnapshotPath()
	tmpPath := snapshotPath + ".tmp"

	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer file.Close()

	cr.messageMu.Lock()
	data, err := json.MarshalIndent(cr.messages, "", "	")
	cr.messageMu.Unlock()

	if err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	file.Close()

	if err := os.Rename(tmpPath, snapshotPath); err != nil {
		return err
	}

	fmt.Printf("Snapshot created (%d messages)\n", len(cr.messages))
	return cr.truncateWAL()
}

func (cr *ChatRoom) truncateWAL() error {
	cr.walMu.Lock()
	defer cr.walMu.Unlock()

	if cr.walFile != nil {
		cr.walFile.Close()
	}

	walPath := cr.getWALPath()
	file, err := os.OpenFile(walPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	cr.walFile = file
	fmt.Println("WAL truncated")
	return nil
}

func (cr *ChatRoom) initializePersistence() error {
	if err := os.MkdirAll(cr.dataDir, 0755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	walPath := cr.getWALPath()
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
	snapshotPath := cr.getSnapshotPath()
	file, err := os.Open(snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	cr.messageMu.Lock()
	err = json.Unmarshal(data, &cr.messages)
	cr.messageMu.Unlock()

	if err != nil {
		return err
	}

	for _, msg := range cr.messages {
		if msg.ID >= cr.nextMessageID {
			cr.nextMessageID = msg.ID + 1
		}
	}

	fmt.Printf("Loaded %d messages from snapshot\n", len(cr.messages))
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
