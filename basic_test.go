// Copyright (c) 2023 BVK Chaitanya

package kvbadger

import (
	"context"
	"testing"
	"time"

	"github.com/bvkgo/kv/kvtests"
	"github.com/dgraph-io/badger/v4"
)

func TestBasicTest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bopts := badger.DefaultOptions(t.TempDir())
	bdb, err := badger.Open(bopts)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	db := New(bdb, nil)
	if err := kvtests.RunBasicOps(ctx, db); err != nil {
		t.Fatal(err)
	}
}

func TestTxSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bopts := badger.DefaultOptions(t.TempDir())
	bdb, err := badger.Open(bopts)
	if err != nil {
		t.Fatal(err)
	}
	defer bdb.Close()

	db := New(bdb, nil)
	if err := kvtests.RunTxOps(ctx, db); err != nil {
		t.Fatal(err)
	}
}
