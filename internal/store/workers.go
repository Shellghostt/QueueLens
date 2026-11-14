package store

import (
	"context"
	"github.com/shellghostt/queuelens/pkg/protocol"
)

func (s *Store) Heartbeat(ctx context.Context, h protocol.Heartbeat) error {
	if err := h.Validate(); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO workers (worker_id,pool) VALUES ($1,$2)
        ON CONFLICT (worker_id) DO UPDATE SET pool=EXCLUDED.pool,last_seen=now()`, h.WorkerID, h.Pool)
	return err
}
