package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/btcsuite/btcd/database/ffldb"
	"github.com/btcsuite/btcd/wire"
)

func recoverDatabase(path string, net wire.BitcoinNet) (uint32, error) {
	var subdir string
	if net == wire.MainNet {
		subdir = "mainnet"
	} else if net == wire.TestNet3 {
		subdir = "testnet"
	}
	p := filepath.Join(filepath.Join(path, subdir), "blocks_ffldb")
	blks, err := ffldb.RecoverDB(p, net)
	if err != nil {
		// Delete the directory in which the new database would have been created.
		os.Remove(filepath.Join(p, "metadata"))
		return 0, err
	}

	return blks, nil
}

func recoverDatabaseFromArgs(args []string) (uint32, error) {
	if len(args) < 1 {
		return 0, errors.New("Must provide database path as only argument.")
	}

	var net wire.BitcoinNet
	if len(args) > 1 {
		switch args[1] {
		case "mainnet":
			net = wire.MainNet
		case "testnet":
			net = wire.TestNet3
		default:
			return 0, errors.New("unrecognized net type")
		}
	} else {
		net = wire.MainNet
	}

	return recoverDatabase(args[0], net)
}

func r(args []string) string {
	blks, err := recoverDatabaseFromArgs(args)
	if err != nil {
		return err.Error()
	}

	return fmt.Sprintf("There were %d blocks read.", blks)
}

func main() {
	println(r(os.Args[1:]))
}
