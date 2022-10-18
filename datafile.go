package bitcask

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MaxFileSize int64 = 1024 * 1024 * 1024;	// 1GB

type DataFile struct {
	sync.RWMutex
	id int64
	r *os.File
	w *os.File
	readonly bool
}

func (df *DataFile) Close() error {
	if err := df.r.Close(); err != nil {
		return err
	}

	if err := df.w.Close(); err != nil {
		return err
	}

	return nil
}

func (df *DataFile) Read() (*Entry, error) {
	hBytes := make([]byte, HeaderSize)
	_, err := df.r.Read(hBytes)
	if err != nil {
		return nil, err
	}
	h := DecodeHeader(hBytes)

	kvBytes := make([]byte, h.keySize + h.valueSize)
	_, err = df.r.Read(kvBytes)
	if err != nil {
		return nil, err
	}

	return Decode(kvBytes, h)
}

func (df *DataFile) ReadValueAt(offset int64, size int32) ([]byte, error) {
	_, err := df.r.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)
	_, err = df.r.Read(buf)

	return buf, err
}

func (df *DataFile) Writable(data []byte) bool {
	stat, err := df.r.Stat()
	if err != nil {
		log.Printf("Failed to get file stat, fileID: %d", df.id)
		return false
	}
	return stat.Size() < MaxFileSize - int64(len(data))
}

func (df *DataFile) Write(key string, value []byte) (*EntryIndex, error) {
	df.Lock()
	defer df.Unlock()

	fileInfo, err := df.w.Stat()
	if err != nil {
		return nil, err
	}

	lastOffset := fileInfo.Size()
	kbytes := []byte(key)
	keySize := uint32(len(kbytes))
	valueSize := uint32(len(value))

	_, err = df.w.Write(Encode(Entry{
		key: kbytes,
		value: value,
		Header: Header{
			keySize: keySize,
			valueSize: valueSize,
			timestamp: uint64(time.Now().Unix()),
		},
	}))

	if err != nil {
		return nil, err
	}

	return &EntryIndex{
		fileID: df.id,
		valueSize: valueSize,
		valuePos: uint64(lastOffset) + uint64(HeaderSize) + uint64(keySize),
	}, nil
}

func (df *DataFile) Delete() error {
	return nil
}

func OpenDataFile(path string, fileID int64, readonly bool) (*DataFile, error) {
	filename := filepath.Join(path, fmt.Sprintf("%s.data", strconv.FormatInt(fileID, 10)))

	r, err := os.OpenFile(filename, os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	
	var w *os.File
	if !readonly {
		w, err = os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, err
		}
	}

	return &DataFile{
		id: fileID,
		r: r,
		w: w,
		readonly: readonly,
	}, nil
}

func GetDataFiles(path string) ([]string, error) {
	filenames, err := filepath.Glob(fmt.Sprintf("%s/*.data", path))
	if err != nil {
		return nil, err
	}
	sort.Strings(filenames)

	return filenames, nil
}

func GetFileID(filename string) (int64, error) {
	bn := filepath.Base(filename)
	ext := filepath.Ext(bn)
	if ext != ".data" {
		return 0, fmt.Errorf("invalid data file %s", bn)
	}
	id, err := strconv.ParseInt(strings.TrimSuffix(bn, ext), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("file ID should be an integer")
	}
	return id, nil
}