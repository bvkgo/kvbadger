// Copyright (c) 2023 BVK Chaitanya

package kvbadger

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"

	"github.com/bvkgo/kv"
	"github.com/dgraph-io/badger/v4"
)

type DB struct {
	db *badger.DB

	checker func(string) bool
}

type Tx struct {
	db *DB

	tx *badger.Txn
}

// New returns a key-value database instance backed by the given badger
// database.
func New(db *badger.DB, keyChecker func(string) bool) *DB {
	return &DB{
		db:      db,
		checker: keyChecker,
	}
}

func (d *DB) checkKey(k string) bool {
	if len(k) == 0 {
		return false
	}
	if d.checker == nil {
		return true
	}
	return d.checker(k)
}

// NewTransaction returns a new read-write transaction.
func (d *DB) NewTransaction(ctx context.Context) (kv.Transaction, error) {
	return &Tx{
		db: d,
		tx: d.db.NewTransaction(true /* update */),
	}, nil
}

// NewSnapshot returns a snapshot, which is just a read-only transaction.
func (d *DB) NewSnapshot(ctx context.Context) (kv.Snapshot, error) {
	return &Tx{
		db: d,
		tx: d.db.NewTransaction(false /* update */),
	}, nil
}

// Commit commits the transaction.
func (t *Tx) Commit(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	t.db = nil
	return t.tx.Commit()
}

// Discard drops the snapshot.
func (t *Tx) Discard(ctx context.Context) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	t.db = nil
	t.tx.Discard()
	return nil
}

// Rollback drops the transaction.
func (t *Tx) Rollback(ctx context.Context) error {
	return t.Discard(ctx)
}

// Get returns the value for a given key.
func (t *Tx) Get(ctx context.Context, k string) (io.Reader, error) {
	if !t.db.checkKey(k) {
		return nil, os.ErrInvalid
	}

	item, err := t.tx.Get([]byte(k))
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	v, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(v), nil
}

// Set stores a key-value pair.
func (t *Tx) Set(ctx context.Context, k string, v io.Reader) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	if !t.db.checkKey(k) {
		return os.ErrInvalid
	}
	data, err := io.ReadAll(v)
	if err != nil {
		return err
	}
	return t.tx.Set([]byte(k), data)
}

// Delete removes the key-value pair with the given key.
func (t *Tx) Delete(ctx context.Context, k string) error {
	if t.db == nil {
		return sql.ErrTxDone
	}
	if !t.db.checkKey(k) {
		return os.ErrInvalid
	}
	if err := t.tx.Delete([]byte(k)); err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return os.ErrNotExist
		}
		return err
	}
	return nil
}

type Iter struct {
	tx *Tx

	it *badger.Iterator

	err   error
	key   string
	value io.Reader

	begin, end string
	descending bool
}

func newIter(ctx context.Context, tx *Tx, it *badger.Iterator, begin, end string, descend bool) (*Iter, error) {
	iter := &Iter{
		tx:         tx,
		it:         it,
		begin:      begin,
		end:        end,
		descending: descend,
	}
	iter.fetch()
	return iter, nil
}

func (it *Iter) setError(err error) {
	it.it.Close()
	it.err = err
}

// Err returns a non-nil error if iteration has encountered any failure.
func (it *Iter) Err() error {
	if !errors.Is(it.err, io.EOF) {
		return it.err
	}
	return nil
}

// Current returns the key-value pair at current iterator position. Returns
// false if iterator has reached the end or encountered any failure.
func (it *Iter) Current(ctx context.Context) (string, io.Reader, bool) {
	if it.err != nil {
		return "", nil, false
	}
	return it.key, it.value, true
}

func (it *Iter) fetch() {
	if !it.it.Valid() {
		it.setError(io.EOF)
		return
	}

	item := it.it.Item()
	k := string(item.KeyCopy(nil))

	done := false
	switch {
	case it.begin != "" && it.end != "":
		if k < it.begin || k >= it.end {
			done = true
		}
	case it.begin == "" && it.end != "":
		if k >= it.end {
			done = true
		}
	case it.begin != "" && it.end == "":
		if k < it.begin {
			done = true
		}
	case it.begin == "" && it.end == "":
		// pass
	}
	if done {
		it.setError(io.EOF)
		return
	}

	v, err := item.ValueCopy(nil)
	if err != nil {
		it.setError(err)
		return
	}

	it.key, it.value = k, bytes.NewReader(v)
}

// Next advances the iterator and returns next key-value pair. Returns
// false when reaches to the end or encounters a failure.
func (it *Iter) Next(ctx context.Context) (string, io.Reader, bool) {
	if it.err != nil {
		return "", nil, false
	}

	it.it.Next()
	it.fetch()

	return it.Current(ctx)
}

// Scan reads all keys in the database through the iterator, in no-particular
// order.
func (t *Tx) Scan(ctx context.Context) (kv.Iterator, error) {
	it := t.tx.NewIterator(badger.DefaultIteratorOptions)
	it.Rewind()

	iter, err := newIter(ctx, t, it, "", "", false /* descend */)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

// Ascend returns key-value pairs in a given range through the iterator, in
// ascending order.
func (t *Tx) Ascend(ctx context.Context, begin, end string) (kv.Iterator, error) {
	if begin > end && end != "" {
		return nil, os.ErrInvalid
	}

	it := t.tx.NewIterator(badger.DefaultIteratorOptions)
	it.Rewind()

	switch {
	case begin == "" && end == "":
		// pass
	case begin != "" && end != "":
		it.Seek([]byte(begin))
	case begin == "" && end != "":
		// pass
	case begin != "" && end == "":
		it.Seek([]byte(begin))
	}

	iter, err := newIter(ctx, t, it, begin, end, false /* descend */)
	if err != nil {
		return nil, err
	}
	return iter, nil
}

// Descend returns key-value pairs in a given range through the iterator, in
// descending order.
func (t *Tx) Descend(ctx context.Context, begin, end string) (kv.Iterator, error) {
	if begin > end && end != "" {
		return nil, os.ErrInvalid
	}

	opts := badger.DefaultIteratorOptions
	opts.Reverse = true

	it := t.tx.NewIterator(opts)
	it.Rewind()

	switch {
	case begin == "" && end == "":
		// pass
	case begin != "" && end != "":
		it.Seek([]byte(end))
		if it.Valid() && string(it.Item().Key()) == end {
			it.Next()
		}
	case begin == "" && end != "":
		it.Seek([]byte(end))
		if it.Valid() && string(it.Item().Key()) == end {
			it.Next()
		}
	case begin != "" && end == "":
		// pass
	}

	iter, err := newIter(ctx, t, it, begin, end, true /* descend */)
	if err != nil {
		return nil, err
	}
	return iter, nil
}
