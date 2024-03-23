// Copyright (c) 2023 BVK Chaitanya

package kvbadger

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bvkgo/kv/kvhttp"
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

	handler := kvhttp.Handler(New(bdb, nil))
	server := httptest.NewServer(handler)
	defer server.Close()

	url, _ := url.Parse(server.URL)
	db := kvhttp.New(url, server.Client())
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

	handler := kvhttp.Handler(New(bdb, nil))
	server := httptest.NewServer(handler)
	defer server.Close()

	url, _ := url.Parse(server.URL)
	db := kvhttp.New(url, server.Client())
	if err := kvtests.RunTxOps(ctx, db); err != nil {
		t.Fatal(err)
	}
}
