package engine

import (
	"fmt"
	"github.com/SimonWaldherr/tinySQL/internal/storage"
)

func (s *querySubscriptionState) checkBudget() error {
	opts := s.options
	if opts.MaxResultRows == 0 && opts.MaxResultBytes == 0 {
		return nil
	}
	count := len(s.rows)
	if s.full {
		count = len(s.snapshot)
	}
	if opts.MaxResultRows > 0 && count > opts.MaxResultRows {
		return fmt.Errorf("subscription result exceeds row limit %d", opts.MaxResultRows)
	}
	if opts.MaxResultBytes == 0 {
		return nil
	}
	var total int64
	check := func(row Row) error {
		data, err := storage.JSONMarshal(row)
		if err != nil {
			return fmt.Errorf("subscription byte accounting: %w", err)
		}
		total += int64(len(data))
		if total > opts.MaxResultBytes {
			return fmt.Errorf("subscription result exceeds byte limit %d", opts.MaxResultBytes)
		}
		return nil
	}
	if s.full {
		for _, row := range s.snapshot {
			if err := check(row); err != nil {
				return err
			}
		}
	} else {
		for _, row := range s.rows {
			if err := check(row); err != nil {
				return err
			}
		}
	}
	return nil
}
