package bitcask

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	HeaderSize = 4 + 8 + 4 + 4;	// Header size in bytes. See the Header struct for more detail
)

type Header struct {
	crc uint32 			// 4 bytes
	timestamp uint64 	// 8 bytes
	keySize uint32		// 4 bytes
	valueSize uint32	// 4 bytes
}

type Entry struct {
	Header
	key []byte
	value []byte
}

func (e *Entry) GetSize() int64 {
	return int64(HeaderSize + e.keySize + e.valueSize)
}

func Encode(e Entry) []byte {
	b := make([]byte, e.GetSize())

	binary.BigEndian.PutUint64(b[4:12], e.timestamp)
	binary.BigEndian.PutUint32(b[12:16], e.keySize)
	binary.BigEndian.PutUint32(b[16:20], e.valueSize)

	copy(b[HeaderSize: HeaderSize + e.keySize], e.key)
	copy(b[HeaderSize + e.keySize: HeaderSize + e.keySize + e.valueSize], e.value)

	crc := crc32.ChecksumIEEE(b[4:])
	binary.BigEndian.PutUint32(b[0:4], crc)

	return b
}

func DecodeHeader(buf []byte) Header {
	crc := binary.BigEndian.Uint32(buf[0:4])
	timestamp := binary.BigEndian.Uint64(buf[4:12])
	keySize := binary.BigEndian.Uint32(buf[12:16])
	valueSize := binary.BigEndian.Uint32(buf[16:20])

	return Header{
		crc: crc,
		timestamp: timestamp,
		keySize: keySize,
		valueSize: valueSize,
	}
}

func Decode(b []byte, h Header) (*Entry, error) {
	return &Entry{
		Header: h,
		key: b[0: h.keySize],
		value: b[h.keySize: h.keySize + h.valueSize],
	}, nil
}
