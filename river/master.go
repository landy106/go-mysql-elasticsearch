package river

import (
	"bytes"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
)

type masterInfo struct {
	sync.RWMutex

	Name string `toml:"bin_name"`
	Pos  uint32 `toml:"bin_pos"`

	filePath     string
	lastSaveTime time.Time
}

func loadMasterInfo(dataDir string) (*masterInfo, error) {
	var m masterInfo

	if len(dataDir) == 0 {
		return &m, nil
	}

	m.filePath = path.Join(dataDir, "master.info")
	m.lastSaveTime = time.Now()

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, errors.Trace(err)
	}

	f, err := os.Open(m.filePath)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return nil, errors.Trace(err)
	} else if os.IsNotExist(errors.Cause(err)) {
		return &m, nil
	}
	defer f.Close()

	// _, err = toml.DecodeReader(f, &m)
	_, err = toml.NewDecoder(f).Decode(&m)
	return &m, errors.Trace(err)
}

func (m *masterInfo) Save(pos mysql.Position) error {
	log.Infof("save position %s", pos)

	m.Lock()
	defer m.Unlock()

	m.Name = pos.Name
	m.Pos = pos.Pos

	if len(m.filePath) == 0 {
		return nil
	}

	n := time.Now()
	if n.Sub(m.lastSaveTime) < time.Second {
		return nil
	}

	m.lastSaveTime = n
	var buf bytes.Buffer
	toml.NewEncoder(&buf).Encode(m)

	var err error
	if err = WriteFileAtomic(m.filePath, buf.Bytes(), 0644); err != nil {
		log.Errorf("canal save master info to file %s err %v", m.filePath, err)
	}

	return errors.Trace(err)
}

func (m *masterInfo) Position() mysql.Position {
	m.RLock()
	defer m.RUnlock()

	return mysql.Position{
		Name: m.Name,
		Pos:  m.Pos,
	}
}

func (m *masterInfo) Close() error {
	pos := m.Position()

	return m.Save(pos)
}

// Write file to temp and atomically move when everything else succeeds.
func WriteFileAtomic(filename string, data []byte, perm os.FileMode) error {
	dir, name := path.Dir(filename), path.Base(filename)

	f, err := os.CreateTemp(dir, name)
	if err != nil {
		return err
	}
	defer f.Close()

	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	} else {
		err = os.Chmod(f.Name(), perm)
	}
	if err != nil {
		os.Remove(f.Name())
		return err
	}
	
	return os.Rename(f.Name(), filename)
}
