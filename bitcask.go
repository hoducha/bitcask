package bitcask

import (
	"errors"
	"io"
	"log"
	"sync"
)

type Bitcask struct {
	sync.Mutex
	path string
	activeFile *DataFile
	closedFiles map[int64]*DataFile
	keydir KeyDir
}

func Open(path string) (*Bitcask, error) {
	filenames, err := GetDataFiles(path)
	if err != nil {
		log.Fatal("GetDataFiles error: ", err)
	}

	k := &KeyDir{ kv: make(map[string]*EntryIndex) }
	closedFiles := make(map[int64]*DataFile)
	var activeFile *DataFile

	for i, filename := range filenames {
		fileID, err := GetFileID(filename)
		if err != nil {
			log.Print("GetFileID error: ", err)
			continue
		}

		readonly := i != len(filenames) - 1
		df, err := OpenDataFile(path, fileID, readonly)
		if err != nil {
			log.Fatal("OpenDataFile error: ", filename)
		}

		if readonly {
			closedFiles[fileID] = df
		} else {
			activeFile = df
		}

		var offset uint64 = 0
		for {
			entry, err := df.Read()
			if err != nil {
				if err == io.EOF {
					break;
				}
				return nil, err
			}
			k.Add(string(entry.key), &EntryIndex{
				fileID: fileID,
				valueSize: entry.valueSize,
				valuePos: offset + uint64(HeaderSize) + uint64(entry.keySize),
				timestamp: entry.timestamp,
			})
		}
	}
	
	if activeFile == nil {
		activeFile, err = OpenDataFile(path, int64(1), false)
		if err != nil {
			return nil, err
		}
	}

	return &Bitcask{
		path: path,
		keydir: *k,
		activeFile: activeFile,
		closedFiles: closedFiles,
	}, nil
}

func (b *Bitcask) Get(key string) ([]byte, error) {
	item := b.keydir.kv[key];
	if item == nil {
		return nil, nil
	}

	var df *DataFile
	if item.fileID == b.activeFile.id {
		df = b.activeFile
	} else {
		df = b.closedFiles[item.fileID]
	}
	if df == nil {
		return nil, nil
	}

	return df.ReadValueAt(int64(item.valuePos), int32(item.valueSize));
}

func (b *Bitcask) Put(key string, value []byte) error {
	if !b.activeFile.Writable(value) {
		df, err := OpenDataFile(b.path, b.activeFile.id + 1, false)
		if err != nil {
			return err
		}
		b.activeFile.readonly = true
		b.closedFiles[b.activeFile.id] = b.activeFile
		b.activeFile = df
	}
	
	entryIndex, err := b.activeFile.Write(key, value)
	if err != nil {
		return err
	}

	b.keydir.Add(key, entryIndex)
	return nil
}

func (b *Bitcask) Delete(key string) error {
	if b.keydir.kv[key] == nil {
		return errors.New("key does not exist")
	}

	err := b.Put(key, []byte{})
	if err != nil {
		return err
	}

	b.keydir.Remove(key)

	return nil
}

func (b *Bitcask) ListKeys() ([]string, error) {
	return []string{}, nil
}

func (b *Bitcask) Close() error {
	if b.activeFile != nil {
		err := b.activeFile.Close()
		if err != nil {
			return err
		}
	}
	if len(b.closedFiles) > 0 {
		for _, f := range b.closedFiles {
			err := f.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *Bitcask) Merge() error {
	return nil
}
