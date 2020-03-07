package ddlog

/*
#include "ddlog.h"
#include <stdlib.h>
#include <assert.h>

ddlog_record **makeRecordArray(size_t s) {
    return malloc(s * sizeof(ddlog_record *));
}

void addRecordToArray(ddlog_record **ra, size_t idx, ddlog_record *r) {
    ra[idx] = r;
}

void freeRecordArray(ddlog_record **ra) {
    free(ra);
}

// a wrapper around ddlog_string_with_length which takes a Go string as parameter.
ddlog_record *ddlogString(_GoString_ s) {
    return ddlog_string_with_length(_GoStringPtr(s), _GoStringLen(s));
}

ddlog_record* ddlogU128(uint64_t lo, uint64_t hi) {
    __uint128_t v = hi;
    v = (v << 64) | lo;
    return ddlog_u128(v);
}

void ddlogGetU128(const ddlog_record *rec, uint64_t *lo, uint64_t *hi) {
    __uint128_t v = ddlog_get_u128(rec);
    *lo = (uint64_t)v;
    *hi = (uint64_t)(v >> 64);
}
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/vmware/differential-datalog/go/pkg/uint128"
)

var (
	// StdSomeConstructor is a static string for the "std.Some" DDlog constructor.
	StdSomeConstructor = NewCString("std.Some")
	// StdNoneConstructor is a static string for the "std.None" DDlog constructor.
	StdNoneConstructor = NewCString("std.None")

	// StdLeftConstructor is a static string for the "std.Left" DDlog constructor.
	StdLeftConstructor = NewCString("std.Left")
	// StdRightConstructor is a static string for the "std.Right" DDlog constructor.
	StdRightConstructor = NewCString("std.Right")
)

// CString is a wrapper around a C string. This is useful when you want to pre-allocate a "static"
// string once and use it multiple times, as it avoids multiple calls to C.CString / copies.
type CString struct {
	ptr *C.char
}

// NewCString creates a new CString. It invokes C.CString which allocates a C string in the C heap
// using malloc and copies the contents of the Go string to that location. Because this is a "C
// pointer", it is not subject to the restrictions of Go pointers
// (https://golang.org/cmd/cgo/#hdr-Passing_pointers). It is the caller's responsibility to release
// the allocated memory by calling Free.
func NewCString(s string) CString {
	return CString{C.CString(s)}
}

// Free releases the memory allocated in the C heap for the underlying C string. Do not use the
// Cstring instance after calling this method.
func (cs CString) Free() {
	C.free(unsafe.Pointer(cs.ptr))
}

// Record represents a DDlog record. It is an interface, rather than simply a wrapper around a
// ddlog_record pointer, to provide some type-safety. In particular, some methods are not included
// in this interface because they are specific to a type of record (e.g. Push() for a RecordVector).
type Record interface {
	ptr() unsafe.Pointer

	// Free releases the memory associated with a given record. Do not call this method if
	// ownership of the record has already been transferred to DDlog (e.g. by adding the record
	// to a command).
	Free()
	// Dump returns a string representation of a record.
	Dump() string

	// IsNull returns true iff the record is NULL.
	IsNull() bool
	// IsBool returns true iff the record is a boolean record.
	IsBool() bool
	// IsInt returns true iff the record is an integer record.
	IsInt() bool
	// IsString returns true iff the record is a string record.
	IsString() bool
	// IsTuple returns true iff the record is a tuple record.
	IsTuple() bool
	// IsVector returns true iff the record is a vector record.
	IsVector() bool
	// IsMap returns true iff the record is a map record.
	IsMap() bool
	// IsSet returns true iff the record is a set record.
	IsSet() bool
	// IsStruct returns true iff the record is a struct record.
	IsStruct() bool

	// IntBits returns the minimum number of bits required to represent the record if it is an
	// integer record. It returns 0 if the record is not an integer record.
	IntBits() int

	// ToBool returns the value of a boolean record. Behavior is undefined if the record is not
	// a boolean.
	ToBool() bool
	// ToBoolSafe returns the value of a boolean record. Returns an error if the record is not a
	// boolean.
	ToBoolSafe() (bool, error)
	// ToU128 returns the value of an integer record as a Uint128. Behavior is undefined if the
	// record is not an integer or if its value does not fit into 128 bits.
	ToU128() uint128.Uint128
	// ToU128Safe returns the value of an integer record as a Uint128. Returns an error if the
	// record is not an integer or if its value does not fit into 128 bits.
	ToU128Safe() (uint128.Uint128, error)
	// ToU64 returns the value of an integer record as a uint64. Behavior is undefined if the
	// record is not an integer or if its value does not fit into 64 bits.
	ToU64() uint64
	// ToU64Safe returns the value of an integer record as a uint64. Returns an error if the
	// record is not an integer or if its value does not fit into 64 bits.
	ToU64Safe() (uint64, error)
	// ToU32 returns the value of an integer record as a uint32. Behavior is undefined if the
	// record is not an integer or if its value does not fit into 32 bits.
	ToU32() uint32
	// ToU32Safe returns the value of an integer record as a uint32. Returns an error if the
	// record is not an integer or if its value does not fit into 32 bits.
	ToU32Safe() (uint32, error)
	// ToI64 returns the value of an integer record as an int64. Behavior is undefined if the
	// record is not an integer or if its value does not fit into 64 bits.
	ToI64() int64
	// ToI64Safe returns the value of an integer record as an int64. Returns an error if the
	// record is not an integer or if its value does not fit into 64 bits.
	ToI64Safe() (int64, error)
	// ToI32 returns the value of an integer record as an int32. Behavior is undefined if the
	// record is not an integer or if its value does not fit into 32 bits.
	ToI32() int32
	// ToI32Safe returns the value of an integer record as an int32. Returns an error if the
	// record is not an integer or if its value does not fit into 32 bits.
	ToI32Safe() (int32, error)
	// ToString returns the value of a string record. Behavior is undefined if the record is not
	// a string.
	ToString() string
	// ToStringSafe returns the value of a string record. Returns an error if the record is not
	// a string.
	ToStringSafe() (string, error)

	// AsTuple interprets the current record as a tuple, enabling the caller to use methods
	// which are specific to tuples on the returned object. Behavior is undefined if the record
	// is not a tuple.
	AsTuple() RecordTuple
	// AsTupleSafe interprets the current record as a tuple, enabling the caller to use methods
	// which are specific to tuples on the returned object. Returns an error if the record is
	// not a tuple.
	AsTupleSafe() (RecordTuple, error)
	// AsVector interprets the current record as a vector, enabling the caller to use methods
	// which are specific to vectors on the returned object. Behavior is undefined if the record
	// is not a vector.
	AsVector() RecordVector
	// AsVectorSafe interprets the current record as a vector, enabling the caller to use
	// methods which are specific to vectors on the returned object. Returns an error if the
	// record is not a vector.
	AsVectorSafe() (RecordVector, error)
	// AsMap interprets the current record as a map, enabling the caller to use methods which
	// are specific to maps on the returned object. Behavior is undefined if the record is not a
	// map.
	AsMap() RecordMap
	// AsMapSafe interprets the current record as a map, enabling the caller to use methods
	// which are specific to maps on the returned object. Returns an error if the record is not
	// a map.
	AsMapSafe() (RecordMap, error)
	// AsSet interprets the current record as a set, enabling the caller to use methods which
	// are specific to sets on the returned object. Behavior is undefined if the record is not a
	// set.
	AsSet() RecordSet
	// AsSetSafe interprets the current record as a set, enabling the caller to use methods
	// which are specific to sets on the returned object. Returns an error if the record is not
	// a set.
	AsSetSafe() (RecordSet, error)
	// AsStruct interprets the current record as a struct, enabling the caller to use methods
	// which are specific to structs on the returned object. Behavior is undefined if the record
	// is not a struct.
	AsStruct() RecordStruct
	// AsStructSafe interprets the current record as a struct, enabling the caller to use
	// methods which are specific to structs on the returned object. Returns an error if the
	// record is not a struct.
	AsStructSafe() (RecordStruct, error)
}

// RecordTuple extends the Record interface for DDlog records of type tuple.
type RecordTuple interface {
	Record
	// Push appends an element to the tuple.
	Push(rValue Record)
	// At returns the i-th element of the tuple. Returns a NULL record if the tuple has fewer
	// than i elements.
	At(idx int) Record
	// Size returns the number of elements in the tuple.
	Size() int
}

// RecordVector extends the Record interface for DDlog records of type vector.
type RecordVector interface {
	Record
	// Push appends an element to the vector.
	Push(rValue Record)
	// At returns the i-th element of the vector. Returns a NULL record if the vector has fewer
	// than i elements.
	At(idx int) Record
	// Size returns the number of elements in the vector.
	Size() int
}

// RecordMap extends the Record interface for DDlog records of type map.
type RecordMap interface {
	Record
	// Push appends a key-value pair to the map.
	Push(rKey, rValue Record)
	// KeyAt returns the i-th key of the map. Returns a NULL record if the map has fewer than i
	// key-value pairs.
	KeyAt(idx int) Record
	// ValueAt returns the i-th value of the map. Returns a NULL record if the map has fewer
	// than i key-value pairs.
	ValueAt(idx int) Record
	// At returns the i-th key-value pair of the map. Returns a NULL record if the map has fewer
	// than i key-value pairs.
	At(idx int) (Record, Record)
	// Size returns the number of key-value pairs in the map.
	Size() int
}

// RecordSet extends the Record interface for DDlog records of type set.
type RecordSet interface {
	Record
	// Push appends an element to the set.
	Push(rValue Record)
	// At returns the i-th element of the set. Returns a NULL record if the set has fewer than i
	// elements.
	At(idx int) Record
	// Size returns the number of elements in the set.
	Size() int
}

// RecordStruct extends the Record interface for DDlog records of type struct.
type RecordStruct interface {
	Record
	// Name returns the constructor name for the struct.
	Name() string
	// At returns the i-th field of the struct. Returns a NULL record if the struct has fewer
	// than i fields.
	At(idx int) Record
}

type record struct {
	recordPtr unsafe.Pointer
}

type recordTuple struct {
	record
}

type recordVector struct {
	record
}

type recordMap struct {
	record
}

type recordSet struct {
	record
}

type recordStruct struct {
	record
}

func (r *record) ptr() unsafe.Pointer {
	return r.recordPtr
}

func (r *record) Dump() string {
	cs := C.ddlog_dump_record(r.ptr())
	defer C.ddlog_string_free(cs)
	return C.GoString(cs)
}

func (r *record) Free() {
	C.ddlog_free(r.ptr())
}

func (r *record) IsNull() bool {
	return r.ptr() == unsafe.Pointer(nil)
}

func (r *record) IsBool() bool {
	return bool(C.ddlog_is_bool(r.ptr()))
}

func (r *record) IsInt() bool {
	return bool(C.ddlog_is_int(r.ptr()))
}

func (r *record) IsString() bool {
	return bool(C.ddlog_is_string(r.ptr()))
}

func (r *record) IsTuple() bool {
	return bool(C.ddlog_is_tuple(r.ptr()))
}

func (r *record) IsVector() bool {
	return bool(C.ddlog_is_vector(r.ptr()))
}

func (r *record) IsMap() bool {
	return bool(C.ddlog_is_map(r.ptr()))
}

func (r *record) IsSet() bool {
	return bool(C.ddlog_is_set(r.ptr()))
}

func (r *record) IsStruct() bool {
	return bool(C.ddlog_is_struct(r.ptr()))
}

func (r *record) IntBits() int {
	return int(C.ddlog_int_bits(r.ptr()))
}

func (r *record) ToBool() bool {
	return bool(C.ddlog_get_bool(r.ptr()))
}

func (r *record) ToBoolSafe() (bool, error) {
	if !r.IsBool() {
		return false, fmt.Errorf("record is not a bool")
	}
	return bool(C.ddlog_get_bool(r.ptr())), nil
}

func (r *record) ToU128() uint128.Uint128 {
	var lo, hi C.uint64_t
	C.ddlogGetU128(r.ptr(), &lo, &hi)
	return uint128.Uint128{Lo: uint64(lo), Hi: uint64(hi)}
}

func (r *record) ToU128Safe() (uint128.Uint128, error) {
	if !r.IsInt() {
		return uint128.Uint128{}, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 128 {
		return uint128.Uint128{}, fmt.Errorf("integer record cannot be represented with 128 bits")
	}
	return r.ToU128(), nil
}

func (r *record) ToU64() uint64 {
	return uint64(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToU64Safe() (uint64, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 64 {
		return 0, fmt.Errorf("integer record cannot be represented with 64 bits")
	}
	return uint64(C.ddlog_get_u64(r.ptr())), nil
}

func (r *record) ToU32() uint32 {
	return uint32(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToU32Safe() (uint32, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 32 {
		return 0, fmt.Errorf("integer record cannot be represented with 32 bits")
	}
	return uint32(C.ddlog_get_u64(r.ptr())), nil
}

func (r *record) ToI64() int64 {
	return int64(C.ddlog_get_u64(r.ptr()))
}

func (r *record) ToI64Safe() (int64, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 64 {
		return 0, fmt.Errorf("integer record cannot be represented with 64 bits")
	}
	return int64(C.ddlog_get_i64(r.ptr())), nil
}

func (r *record) ToI32() int32 {
	return int32(C.ddlog_get_i64(r.ptr()))
}

func (r *record) ToI32Safe() (int32, error) {
	if !r.IsInt() {
		return 0, fmt.Errorf("record is not an integer")
	}
	if r.IntBits() > 32 {
		return 0, fmt.Errorf("integer record cannot be represented with 32 bits")
	}
	return int32(C.ddlog_get_i64(r.ptr())), nil
}

func (r *record) ToString() string {
	var len C.size_t
	cs := C.ddlog_get_str_with_length(r.ptr(), &len)
	return C.GoStringN(cs, C.int(len))
}

func (r *record) ToStringSafe() (string, error) {
	var len C.size_t
	cs := C.ddlog_get_str_with_length(r.ptr(), &len)
	if unsafe.Pointer(cs) == unsafe.Pointer(nil) {
		return "", fmt.Errorf("record is not a string")
	}
	return C.GoStringN(cs, C.int(len)), nil
}

func (r *record) AsTuple() RecordTuple {
	return &recordTuple{*r}
}

func (r *record) AsTupleSafe() (RecordTuple, error) {
	if !r.IsTuple() {
		return nil, fmt.Errorf("record is not a tuple")
	}
	return &recordTuple{*r}, nil
}

func (r *record) AsVector() RecordVector {
	return &recordVector{*r}
}

func (r *record) AsVectorSafe() (RecordVector, error) {
	if !r.IsVector() {
		return nil, fmt.Errorf("record is not a vector")
	}
	return &recordVector{*r}, nil
}

func (r *record) AsMap() RecordMap {
	return &recordMap{*r}
}

func (r *record) AsMapSafe() (RecordMap, error) {
	if !r.IsMap() {
		return nil, fmt.Errorf("record is not a map")
	}
	return &recordMap{*r}, nil
}

func (r *record) AsSet() RecordSet {
	return &recordSet{*r}
}

func (r *record) AsSetSafe() (RecordSet, error) {
	if !r.IsSet() {
		return nil, fmt.Errorf("record is not a set")
	}
	return &recordSet{*r}, nil
}

func (r *record) AsStruct() RecordStruct {
	return &recordStruct{*r}
}

func (r *record) AsStructSafe() (RecordStruct, error) {
	if !r.IsStruct() {
		return nil, fmt.Errorf("record is not a struct")
	}
	return &recordStruct{*r}, nil
}

// NewRecordBool creates a boolean record.
func NewRecordBool(v bool) Record {
	r := C.ddlog_bool(C.bool(v))
	return &record{r}
}

// NewRecordU128 creates a record for an unsigned integer value. Can be used to populate any DDlog
// field of type `bit<N>`, `N<=128`.
func NewRecordU128(v uint128.Uint128) Record {
	r := C.ddlogU128(C.uint64_t(v.Lo), C.uint64_t(v.Hi))
	return &record{r}
}

// NewRecordU64 creates a record for an unsigned integer value. Can be used to populate any DDlog
// field of type `bit<N>`, `N<=64`.
func NewRecordU64(v uint64) Record {
	r := C.ddlog_u64(C.uint64_t(v))
	return &record{r}
}

// NewRecordU32 creates a record for an unsigned integer value. Can be used to populate any DDlog
// field of type `bit<N>`, `N<=32`.
func NewRecordU32(v uint32) Record {
	return NewRecordU64(uint64(v))
}

// NewRecordI64 creates a record for a signed integer value. Can be used to populate any DDlog field
// of type `signed<N>`, `N<=64`.
func NewRecordI64(v int64) Record {
	r := C.ddlog_i64(C.int64_t(v))
	return &record{r}
}

// NewRecordI32 creates a record for a signed integer value. Can be used to populate any DDlog field
// of type `signed<N>`, `N<=32`.
func NewRecordI32(v int32) Record {
	return NewRecordI64(int64(v))
}

// NewRecordString creates a record for a string.
func NewRecordString(v string) Record {
	r := C.ddlogString(v)
	return &record{r}
}

// NewRecordStruct creates a struct record with specified constructor name and arguments.
func NewRecordStruct(constructor string, records ...Record) RecordStruct {
	cs := C.CString(constructor)
	defer C.free(unsafe.Pointer(cs))
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_struct(cs, recordArray, C.size_t(len(records)))
	return &recordStruct{record{r}}
}

// NewRecordStructStatic creates a struct record with specified constructor name and
// arguments. Unlike NewRecordStruct, this function takes a CString for the constructor to avoid
// making an extra copy of the constructor string when it is "static" (known ahead of time).
func NewRecordStructStatic(constructor CString, records ...Record) RecordStruct {
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_struct_static_cons(constructor.ptr, recordArray, C.size_t(len(records)))
	return &recordStruct{record{r}}
}

func (rStruct *recordStruct) Name() string {
	var len C.size_t
	cs := C.ddlog_get_constructor_with_length(rStruct.ptr(), &len)
	return C.GoStringN(cs, C.int(len))
}

func (rStruct *recordStruct) At(idx int) Record {
	r := C.ddlog_get_struct_field(rStruct.ptr(), C.size_t(idx))
	return &record{r}
}

// NewRecordTuple creates a tuple record with specified fields.
func NewRecordTuple(records ...Record) RecordTuple {
	// avoid unnecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_tuple(nil, 0)
		return &recordTuple{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_tuple(recordArray, C.size_t(len(records)))
	return &recordTuple{record{r}}
}

func (rTuple *recordTuple) Push(rValue Record) {
	C.ddlog_tuple_push(rTuple.ptr(), rValue.ptr())
}

func (rTuple *recordTuple) At(idx int) Record {
	r := C.ddlog_get_tuple_field(rTuple.ptr(), C.size_t(idx))
	return &record{r}
}

func (rTuple *recordTuple) Size() int {
	return int(C.ddlog_get_tuple_size(rTuple.ptr()))
}

// NewRecordPair is a convenience way to create a 2-tuple. Such tuples are useful when constructing
// maps out of key-value pairs.
func NewRecordPair(r1, r2 Record) RecordTuple {
	r := C.ddlog_pair(r1.ptr(), r2.ptr())
	return &recordTuple{record{r}}
}

// NewRecordMap creates a map record with specified key-value pairs.
func NewRecordMap(records ...Record) RecordMap {
	// avoid unnecessary C calls if we are creating an empty map
	if len(records) == 0 {
		r := C.ddlog_map(nil, 0)
		return &recordMap{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_map(recordArray, C.size_t(len(records)))
	return &recordMap{record{r}}
}

// RecordMapPush appends a key-value pair to a map.
// func RecordMapPush(rMap, rKey, rValue Record) {
// 	C.ddlog_map_push(rMap.ptr(), rKey.ptr(), rValue.ptr())
// }

func (rMap *recordMap) Push(rKey, rValue Record) {
	C.ddlog_map_push(rMap.ptr(), rKey.ptr(), rValue.ptr())
}

func (rMap *recordMap) KeyAt(idx int) Record {
	r := C.ddlog_get_map_key(rMap.ptr(), C.size_t(idx))
	return &record{r}
}

func (rMap *recordMap) ValueAt(idx int) Record {
	r := C.ddlog_get_map_val(rMap.ptr(), C.size_t(idx))
	return &record{r}
}

func (rMap *recordMap) At(idx int) (Record, Record) {
	return rMap.KeyAt(idx), rMap.ValueAt(idx)
}

func (rMap *recordMap) Size() int {
	return int(C.ddlog_get_map_size(rMap.ptr()))
}

// NewRecordVector creates a vector record with specified elements.
func NewRecordVector(records ...Record) RecordVector {
	// avoid unnecessary C calls if we are creating an empty vector
	if len(records) == 0 {
		r := C.ddlog_vector(nil, 0)
		return &recordVector{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_vector(recordArray, C.size_t(len(records)))
	return &recordVector{record{r}}
}

// Push appends an element to a vector.
func (rVec *recordVector) Push(rValue Record) {
	C.ddlog_vector_push(rVec.ptr(), rValue.ptr())
}

func (rVec *recordVector) At(idx int) Record {
	r := C.ddlog_get_vector_elem(rVec.ptr(), C.size_t(idx))
	return &record{r}
}

func (rVec *recordVector) Size() int {
	return int(C.ddlog_get_vector_size(rVec.ptr()))
}

// NewRecordSet creates a set record with specified elements.
func NewRecordSet(records ...Record) RecordSet {
	// avoid unnecessary C calls if we are creating an empty set
	if len(records) == 0 {
		r := C.ddlog_set(nil, 0)
		return &recordSet{record{r}}
	}
	recordArray := C.makeRecordArray(C.size_t(len(records)))
	defer C.freeRecordArray(recordArray)
	for idx, record := range records {
		C.addRecordToArray(recordArray, C.size_t(idx), record.ptr())
	}
	r := C.ddlog_set(recordArray, C.size_t(len(records)))
	return &recordSet{record{r}}
}

// Push appends an element to a set.
func (rSet *recordSet) Push(rValue Record) {
	C.ddlog_set_push(rSet.ptr(), rValue.ptr())
}

func (rSet *recordSet) At(idx int) Record {
	r := C.ddlog_get_set_elem(rSet.ptr(), C.size_t(idx))
	return &record{r}
}

func (rSet *recordSet) Size() int {
	return int(C.ddlog_get_set_size(rSet.ptr()))
}

// NewRecordSome is a convenience wrapper around NewRecordStructStatic for the std.Some
// constructor.
func NewRecordSome(r Record) Record {
	return NewRecordStructStatic(StdSomeConstructor, r)
}

// NewRecordNone is a convenience wrapper around NewRecordStructStatic for the std.None
// constructor.
func NewRecordNone() Record {
	return NewRecordStructStatic(StdNoneConstructor)
}

// NewRecordNull returns a NULL record, which can be used as a placeholder for an invalid record.
func NewRecordNull() Record {
	return &record{nil}
}

// NewRecordLeft is a convenience wrapper around NewRecordStructStatic for the std.Left
// constructor.
func NewRecordLeft(r Record) Record {
	return NewRecordStructStatic(StdLeftConstructor, r)
}

// NewRecordRight is a convenience wrapper around NewRecordStructStatic for the std.Right
// constructor.
func NewRecordRight(r Record) Record {
	return NewRecordStructStatic(StdRightConstructor, r)
}
