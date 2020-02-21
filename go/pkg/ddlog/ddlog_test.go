package ddlog

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// These tests are written for the test/types_test/typesTest.dl program.

const (
	numDDlogWorkers = 2
)

func TestGetTableID(t *testing.T) {
	// input relation BI(b: bool)
	tableID := GetTableID("BI")
	tableName := GetTableName(tableID)
	assert.Equal(t, "BI", tableName)
}

type mockHandler struct {
	mock.Mock
	test *testing.T
}

func (m *mockHandler) Handle(tableID TableID, r Record, outPolarity OutPolarity) {
	m.Called(tableID, r, outPolarity)
}

func TestInsert(t *testing.T) {
	m := &mockHandler{test: t}

	ddlogProgram, err := NewProgram(numDDlogWorkers, m)
	assert.Nil(t, err, "Error when running DDlog program")

	tableID := GetTableID("L0I")
	outTableID := GetTableID("OL0I")
	constructor := NewCString("L0I")
	defer constructor.Free()
	rStruct := NewRecordStructStatic(
		constructor,
		NewRecordBool(true),
		NewRecordU32(77),
		NewRecordString("DDlog"),
	)
	cmd := NewInsertCommand(tableID, rStruct)

	// will panic if the call is not the expected one, which is not ideal.
	// TODO: fail the test instead, but FailNow() has to be called from the main goroutine.
	m.On(
		"Handle",
		outTableID,
		mock.MatchedBy(func(r Record) bool { return r.IsStruct() }),
		OutPolarityInsert,
	).Return()

	err = ddlogProgram.ApplyUpdatesAsTransaction(cmd)
	assert.Nil(t, err, "Error when processing transaction")

	err = ddlogProgram.Stop()
	assert.Nil(t, err, "Error when stopping DDlog program")

	m.AssertExpectations(t)
}

func BenchmarkTransaction(b *testing.B) {
	outRecordHandler, err := NewOutRecordSink()
	assert.Nil(b, err)
	ddlogProgram, err := NewProgram(numDDlogWorkers, outRecordHandler)
	assert.Nil(b, err, "Error when running DDlog program")

	tableID := GetTableID("ZI21")
	constructor := NewCString("ZI21")
	defer constructor.Free()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err = ddlogProgram.StartTransaction()
		assert.Nil(b, err)
		for j := 0; j < 100; j++ {
			rStruct := NewRecordStructStatic(
				constructor,
				NewRecordU32(rand.Uint32()),
			)
			cmd := NewInsertCommand(tableID, rStruct)
			ddlogProgram.ApplyUpdate(cmd)
		}
		err = ddlogProgram.CommitTransaction()
		assert.Nil(b, err)
	}

	b.StopTimer()

	err = ddlogProgram.Stop()
	assert.Nil(b, err, "Error when stopping DDlog program")
}
