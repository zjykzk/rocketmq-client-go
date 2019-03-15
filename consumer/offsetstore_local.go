package consumer

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/zjykzk/rocketmq-client-go/message"
)

type localStore struct {
	*baseStore

	path string
}

type localStoreConfig struct {
	rootPath string
	clientID string
	group    string
}

func newLocalStore(conf localStoreConfig) (*localStore, error) {
	if conf.rootPath == "" {
		root, err := Home()
		if err != nil {
			return nil, err
		}
		conf.rootPath = root
	}

	if conf.clientID == "" {
		return nil, errors.New("newLocalStore error:empty clientID")
	}

	if conf.group == "" {
		return nil, errors.New("newLocalStore error:empty group")
	}

	ls := &localStore{
		baseStore: &baseStore{Offsets: make([]*offsets, 0, 8)},
		path:      filepath.Join(conf.rootPath, conf.clientID, conf.group, "offsets.json"),
	}
	ls.baseStore.readOffsetFromStore = ls.readOffsetFromStore

	ls.load()
	return ls, nil
}

func (ls *localStore) load() error {
	of, err := loadFromFiles(ls.path)
	if err != nil {
		return err
	}
	ls.Offsets = of
	return nil
}

// loadFromFiles returns the offset data
// if the path is not exist, try to load from the bak file
func loadFromFiles(path string) (of []*offsets, err error) {
	of, err = loadFromFile(path)
	if err == nil {
		return
	}

	if !os.IsNotExist(err) { // ignore the file not exist
		return
	}

	of, err = loadFromFile(bakPath(path))
	if os.IsNotExist(err) { // ignore the file not exist
		err = nil
	}
	return
}

func loadFromFile(path string) (of []*offsets, err error) {
	file, err := os.Open(path)
	if err != nil {
		return
	}

	d, err := ioutil.ReadAll(file)
	if err != nil {
		return
	}

	err = json.Unmarshal(d, &of)
	return
}

func (ls *localStore) readOffsetFromStore(q *message.Queue) (int64, error) {
	offsets, err := loadFromFiles(ls.path)
	if err != nil {
		return 0, err
	}

	offset, ok := readOffset(offsets, q)
	if ok {
		ls.updateOffset(q, offset)
	}
	return offset, nil
}

func (ls *localStore) persist() error {
	if err := ls.makeSureDir(); err != nil {
		return err
	}

	// write to temporary file
	d, err := json.Marshal(ls.Offsets)
	if err != nil {
		return err
	}
	tmpName := ls.path + ".tmp"
	f, err := os.OpenFile(tmpName, os.O_RDWR|os.O_CREATE, 0666)
	_, err = f.Write(d)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}

	// backup
	f, err = os.Open(ls.path)
	if os.IsNotExist(err) {
		goto RENAME
	}
	if err != nil {
		return err
	}
	d, err = ioutil.ReadAll(f)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}

	f, err = os.OpenFile(bakPath(ls.path), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	_, err = f.Write(d)
	if err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}

RENAME:
	return os.Rename(tmpName, ls.path)
}

func (ls *localStore) makeSureDir() error {
	return os.MkdirAll(filepath.Dir(ls.path), os.ModePerm)
}

func (ls *localStore) updateQueues(qs ...*message.Queue) {
	// DO NOTHING
}

func bakPath(path string) string {
	return path + ".bak"
}

func (ls *localStore) persistOne(mq *message.Queue) {
	// DO NOTHING
}
func (ls *localStore) removeOffset(mq *message.Queue) (offset int64, ok bool) {
	// DO NOTHING
	return
}
