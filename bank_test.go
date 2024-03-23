// Copyright (c) 2023 BVK Chaitanya

package kvbadger

import (
	"context"
	"errors"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/bvkgo/kv/kvhttp"
	"github.com/bvkgo/kv/kvtests"
	"github.com/dgraph-io/badger/v4"
)

func TestBankTransactions(t *testing.T) {
	ctx := context.Background()
	duration := 10 * time.Second

	tctx, tcancel := context.WithTimeout(ctx, duration)
	defer tcancel()

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

	b := kvtests.BankTest{
		DB:           db,
		InitializeDB: true,
	}

	if err := b.Run(tctx, 1000); err != nil {
		if !errors.Is(err, tctx.Err()) {
			t.Fatal(err)
		}
	}
}
