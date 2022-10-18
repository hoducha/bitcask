package bitcask

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func CreateDataDir() (string, error) {
	name := "bitcask-" + strconv.FormatInt(time.Now().Unix(), 10)
	path := filepath.Join(os.TempDir(), name)
	err := os.Mkdir(path, 0777)
	if err != nil {
		return "", err
	}
	return path, nil
}

func RemoveDataDir(path string) error {
	return os.RemoveAll(path)
}

func TestBasicOperations(t *testing.T) {
	assert := assert.New(t)
	var (
		b *Bitcask
		path string
		err error
	)
	path, err = CreateDataDir()
	require.Empty(t, err)

	t.Run("Open", func(t *testing.T) {
		b, err = Open(path)
		assert.NoError(err)
	})

	key := "item-key"
	value := "item-value"

	t.Run("Put", func(t *testing.T) {
		err := b.Put(key, []byte(value))
		assert.NoError(err)
	})

	t.Run("Get", func(t *testing.T) {
		data, err := b.Get(key)
		assert.NoError(err)
		assert.Equal(value, string(data))
	})

	t.Run("Update", func(t *testing.T) {
		newValue := "new-item-value"
		err := b.Put(key, []byte(newValue))
		assert.NoError(err)

		data, err := b.Get(key)
		assert.NoError(err)
		assert.Equal(newValue, string(data))
	})

	t.Run("Delete", func(t *testing.T) {
		err := b.Delete(key)
		assert.NoError(err)

		data, err := b.Get(key)
		assert.NoError(err)
		assert.Nil(data)

		err = b.Delete(key)
		assert.Equal("key does not exist", err.Error())
	})

	t.Run("Close", func(t *testing.T) {
		err := b.Close()
		assert.NoError(err)
	})

	t.Cleanup(func() {
		RemoveDataDir(path)
	})
}

func BenchmarkGet(b *testing.B) {
	assert := assert.New(b)
	path, err := CreateDataDir()
	require.Empty(b, err)

	db, err := Open(path)
	assert.NoError(err)

	key := "item-key"
	value := "item-value"
	db.Put(key, []byte(value))

	for i := 0; i < b.N; i++ {
		db.Get(key)
	}

	b.Cleanup(func() {
		RemoveDataDir(path)
	})
}

func BenchmarkPut(b *testing.B) {
	assert := assert.New(b)
	path, err := CreateDataDir()
	require.Empty(b, err)

	db, err := Open(path)
	assert.NoError(err)

	key := "item-key"
	value := "item-value"

	for i := 0; i < b.N; i++ {
		idx := fmt.Sprint(i)
		db.Put(key + idx, []byte(value + idx))
	}

	b.Cleanup(func() {
		RemoveDataDir(path)
	})
}