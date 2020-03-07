package uint128

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/google/uuid"
)

// Uint128 stores a 128-bit unsigned integer as two 64-bit words. Two Uint128 values can be compared
// with ==.
type Uint128 struct {
	// least significant word
	Lo uint64
	// most significant word
	Hi uint64
}

// Zero is a Uint128 with numerical value 0.
var Zero = Uint128{0, 0}

// String returns the hexadecimal string representation for a Uint128.
func (u Uint128) String() string {
	var b [16]byte
	u.PutBytesBE(b[:])
	return hex.EncodeToString(b[:])
}

// FromBytesLE converts the provided slice to a Uint128. Bytes are read from the slice in
// little-endian order. It will panic if the length of the slice is not 16.
func FromBytesLE(b []byte) Uint128 {
	return Uint128{
		Lo: binary.LittleEndian.Uint64(b[:8]),
		Hi: binary.LittleEndian.Uint64(b[8:]),
	}
}

// FromBytesBE converts the provided slice to a Uint128. Bytes are read from the slice in big-endian
// order. It will panic if the length of the slice is not 16.
func FromBytesBE(b []byte) Uint128 {
	return Uint128{
		Lo: binary.BigEndian.Uint64(b[8:]),
		Hi: binary.BigEndian.Uint64(b[:8]),
	}
}

// PutBytesLE puts the bytes from the provided Uint128 in little-endian order. It will panic if the
// length of the slice is not 16.
func (u Uint128) PutBytesLE(b []byte) {
	binary.LittleEndian.PutUint64(b[:8], u.Lo)
	binary.LittleEndian.PutUint64(b[8:], u.Hi)
}

// PutBytesBE puts the bytes from the provided Uint128 in big-endian order. It will panic if the
// length of the slice is not 16.
func (u Uint128) PutBytesBE(b []byte) {
	binary.BigEndian.PutUint64(b[:8], u.Hi)
	binary.BigEndian.PutUint64(b[8:], u.Lo)
}

// AsUUID converts the provided Uint128 to a UUID.
func (u Uint128) AsUUID() uuid.UUID {
	var b [16]byte
	u.PutBytesBE(b[:])
	return b
}

// FromUUID converts the provided UUID to a Uint128.
func FromUUID(UUID uuid.UUID) Uint128 {
	return FromBytesBE(UUID[:])
}
