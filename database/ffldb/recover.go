// Copyright (c) 2015-2016 The btcsuite developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package ffldb

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/database"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/filter"
	"github.com/btcsuite/goleveldb/leveldb/opt"
)

var zeroHash chainhash.Hash

type scanner struct {
	s       *blockStore
	fileNum uint32
	fileOff uint32
	fileLen uint32
}

func (s scanner) getNextLocation() blockLocation {
	return blockLocation{blockFileNum: s.fileNum, fileOffset: s.fileOff, blockLen: 0}
}

func (s scanner) getNextBlock() (scanner, *btcutil.Block, blockLocation, error) {
	if s.s == nil {
		return scanner{}, nil, blockLocation{}, nil
	}

	next := s
	old := next.getNextLocation()

	// if the length of the file is zero, we have to figure out what
	// the length is so that we know when to move on to the next file.
	if next.fileLen == 0 {
		filePath := blockFilePath(s.s.basePath, s.fileNum)
		st, err := os.Stat(filePath)

		// if the file does not exist, that means we have just
		// reached the end of the list.
		if err != nil {
			return scanner{}, nil, blockLocation{}, nil
		}

		next.fileLen = uint32(st.Size())
	}

	block, err := s.s.readBlock(&zeroHash, old)
	if err != nil {
		return scanner{}, nil, blockLocation{}, err
	}

	var msgBlock wire.MsgBlock
	msgBlock.Deserialize(bytes.NewBuffer(block))

	// 12 is added to the offest to account for the extra metadata stored in the
	// block database.
	old.blockLen = uint32(len(block)) + 12
	next.fileOff += old.blockLen

	if next.fileOff == next.fileLen {
		next.fileLen = 0
		next.fileOff = 0
		next.fileNum++
	}

	return next, btcutil.NewBlock(&msgBlock), old, nil
}

// recoverDB takes a leveldb database that doesn't know about any of the blocks
// stored in the flat files and goes through all the flat files
func recoverDB(db *db) (blocksRead uint32, err error) {
	sc := scanner{s: db.store}
	var location blockLocation
	var blk *btcutil.Block
	var tx *transaction
	tx, err = db.begin(true)
	if err != nil {
		return
	}
	for {
		sc, blk, location, err = sc.getNextBlock()
		if err != nil {
			return
		}
		if blk == nil {
			break
		}

		tx.storeBlockRecord(blk.Hash(), location)

		blocksRead++
	}

	wc := db.store.writeCursor

	// Update the metadata for the current write file and offset.
	writeRow := serializeWriteRow(wc.curFileNum, wc.curOffset)
	if err = tx.metaBucket.Put(writeLocKeyName, writeRow); err != nil {
		err = convertErr("failed to store write cursor", err)
		return
	}

	err = tx.db.cache.commitTx(tx)
	if err != nil {
		return
	}

	return
}

func RecoverDB(dbPath string, network wire.BitcoinNet) (uint32, error) {
	// Error if the database exists.
	metadataDbPath := filepath.Join(dbPath, metadataDbName)
	dbExists := fileExists(metadataDbPath)
	if dbExists {
		str := fmt.Sprintf("database %q exists", metadataDbPath)
		return 0, makeDbErr(database.ErrDbExists, str, nil)
	}

	// Ensure the full path to the database exists.
	if !dbExists {
		// The error can be ignored here since the call to
		// leveldb.OpenFile will fail if the directory couldn't be
		// created.
		_ = os.MkdirAll(dbPath, 0700)
	}

	// Open the metadata database (will create it if needed).
	opts := opt.Options{
		ErrorIfExist: false,
		Strict:       opt.DefaultStrict,
		Compression:  opt.NoCompression,
		Filter:       filter.NewBloomFilter(10),
	}
	ldb, err := leveldb.OpenFile(metadataDbPath, &opts)
	if err != nil {
		return 0, convertErr(err.Error(), err)
	}

	store := newBlockStore(dbPath, network)
	cache := newDbCache(ldb, store, defaultCacheSize, defaultFlushSecs)
	pdb := &db{store: store, cache: cache}

	if err := initDB(pdb.cache.ldb); err != nil {
		return 0, err
	}

	return recoverDB(pdb)
}
