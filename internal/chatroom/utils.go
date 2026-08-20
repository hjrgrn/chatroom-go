package chatroom

import "path/filepath"

func (cr *ChatRoom) getSnapshotPath() string {
	return filepath.Join(cr.dataDir, "snapshot.json")
}

func (cr *ChatRoom) getWALPath() string {
	return filepath.Join(cr.dataDir, "messages.wal")
}
