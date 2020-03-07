package ddlog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/vmware/differential-datalog/go/pkg/uint128"
)

// In theory these tests do not depend on any specific DDlog program since DDlog does not perform
// any validation when building records. However running the tests require the ddlog.h header and an
// actual .so built for a DDlog program.

func TestRecordEmptyString(t *testing.T) {
	emptyString := ""
	r := NewRecordString(emptyString)
	defer r.Free()
	assert.False(t, r.IsNull())
}

func TestRecordInteger(t *testing.T) {
	v := uint64(1) << 48
	r := NewRecordU64(v)
	defer r.Free()

	assert.True(t, r.IsInt())
	assert.Equal(t, 49, r.IntBits())
	assert.Equal(t, v, r.ToU64())
	v64, err := r.ToU64Safe()
	assert.Nil(t, err)
	assert.Equal(t, v, v64)

	v32, err := r.ToU32Safe()
	assert.NotNil(t, err)
	assert.Equal(t, uint32(0), v32)
}

func TestRecordUint128(t *testing.T) {
	v := uint128.Uint128{Lo: 0xB65ABF568A99CCB5, Hi: 0x208F3AFD4FAF5761}
	r := NewRecordU128(v)
	defer r.Free()

	assert.True(t, r.IsInt())
	// 0x20 -> 0010 0000
	assert.Equal(t, 126, r.IntBits())
	assert.Equal(t, v, r.ToU128())
	v128, err := r.ToU128Safe()
	assert.Nil(t, err)
	assert.Equal(t, v, v128)

	_, err = r.ToU64Safe()
	assert.NotNil(t, err)
}

func TestRecordVector(t *testing.T) {
	// Vec<bool>
	rVec := NewRecordVector(NewRecordBool(false), NewRecordBool(true))
	defer rVec.Free()

	rVec.Push(NewRecordBool(false))

	assert.Equal(t, 3, rVec.Size())
	rBool := rVec.At(1) // true
	assert.Equal(t, true, rBool.ToBool())

	// out-of-bound access
	assert.True(t, rVec.At(99).IsNull())

	// erase type
	r := rVec.(Record)
	assert.True(t, r.IsVector())
	rVec = r.AsVector()
	assert.Equal(t, 3, rVec.Size())
	rVec, err := r.AsVectorSafe()
	assert.Nil(t, err)
	assert.Equal(t, 3, rVec.Size())
}

func TestRecordTuple(t *testing.T) {
	// Tuple<bool>
	rTuple := NewRecordTuple(NewRecordBool(false), NewRecordBool(true))
	defer rTuple.Free()

	rTuple.Push(NewRecordBool(false))

	assert.Equal(t, 3, rTuple.Size())
	rBool := rTuple.At(1) // true
	assert.Equal(t, true, rBool.ToBool())

	// out-of-bound access
	assert.True(t, rTuple.At(99).IsNull())

	// erase type
	r := rTuple.(Record)
	assert.True(t, r.IsTuple())
	rTuple = r.AsTuple()
	assert.Equal(t, 3, rTuple.Size())
	rTuple, err := r.AsTupleSafe()
	assert.Nil(t, err)
	assert.Equal(t, 3, rTuple.Size())
}

func TestRecordSet(t *testing.T) {
	// Set<bool>
	// a set is just a vector until it is sent to DDlog.
	rSet := NewRecordSet(NewRecordBool(false), NewRecordBool(true))
	defer rSet.Free()

	rSet.Push(NewRecordBool(false))

	// DDlog does not filter duplicates when building records.
	assert.Equal(t, 3, rSet.Size())
	rBool := rSet.At(1) // true
	assert.Equal(t, true, rBool.ToBool())

	// out-of-bound access
	assert.True(t, rSet.At(99).IsNull())

	// erase type
	r := rSet.(Record)
	assert.True(t, r.IsSet())
	rSet = r.AsSet()
	assert.Equal(t, 3, rSet.Size())
	rSet, err := r.AsSetSafe()
	assert.Nil(t, err)
	assert.Equal(t, 3, rSet.Size())
}

func TestRecordMap(t *testing.T) {
	// Map<bit<32>, string>
	// a map is just a vector of pairs until it is sent to DDlog.
	rMap := NewRecordMap()
	defer rMap.Free()

	entries := map[uint32]string{
		1: "DDlog",
		2: "is",
		3: "awesome",
	}

	for k, v := range entries {
		rMap.Push(NewRecordU32(k), NewRecordString(v))
	}
	assert.EqualValues(t, len(entries), rMap.Size())

	rKey := rMap.KeyAt(1)
	assert.True(t, rKey.IsInt())
	mKey := rKey.ToU32()
	rVal := rMap.ValueAt(1)
	assert.True(t, rVal.IsString())
	assert.Equal(t, entries[mKey], rVal.ToString())
	rKey, rVal = rMap.At(1)
	assert.Equal(t, mKey, rKey.ToU32())
	assert.Equal(t, entries[mKey], rVal.ToString())

	// DDlog does not filter duplicates when building records.
	rMap.Push(NewRecordU32(1), NewRecordString(entries[1]))
	assert.EqualValues(t, len(entries)+1, rMap.Size())

	// erase type
	r := rMap.(Record)
	assert.True(t, r.IsMap())
	rMap = r.AsMap()
	assert.EqualValues(t, len(entries)+1, rMap.Size())
	rMap, err := r.AsMapSafe()
	assert.Nil(t, err)
	assert.EqualValues(t, len(entries)+1, rMap.Size())
}

func TestRecordStruct(t *testing.T) {
	// input relation L0I(a: bool, b: bit<8>, s: string)
	constructor := NewCString("L0I")
	defer constructor.Free()
	rStruct := NewRecordStructStatic(
		constructor,
		NewRecordBool(true),
		NewRecordU32(77),
		NewRecordString("DDlog"),
	)
	defer rStruct.Free()

	assert.Equal(t, "L0I", rStruct.Name())
	rString := rStruct.At(2)
	assert.True(t, rString.IsString())
	assert.Equal(t, "DDlog", rString.ToString())

	// erase type
	r := rStruct.(Record)
	assert.True(t, r.IsStruct())
	rStruct = r.AsStruct()
	assert.Equal(t, "L0I", rStruct.Name())
	rStruct, err := r.AsStructSafe()
	assert.Nil(t, err)
	assert.Equal(t, "L0I", rStruct.Name())
}

func BenchmarkRecord(b *testing.B) {
	// TODO: use a more complex relation, e.g. with a vector as a field
	// input relation L0I(a: bool, b: bit<8>, s: string)
	constructor := NewCString("L0I")
	defer constructor.Free()
	for i := 0; i < b.N; i++ {
		rStruct := NewRecordStructStatic(
			constructor,
			NewRecordBool(true),
			NewRecordU32(77),
			NewRecordString("DDlog"),
		)
		rStruct.Dump()
		rStruct.Free()
	}
}
