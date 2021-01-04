package ddlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// These tests are written for the test/types_test/typesTest.dl program.

const (
	numDDlogWorkers = 2
)

type mockHandler struct {
	mock.Mock
	test *testing.T
}

func (m *mockHandler) Handle(p *Program, tableID TableID, r Record, weight int64) {
	m.Called(tableID, r, weight)
}

func TestInsert(t *testing.T) {
	m := &mockHandler{test: t}

	ddlogProgram, err := NewProgram(numDDlogWorkers, m)
	assert.Nil(t, err, "Error when running DDlog program")

	biID := ddlogProgram.GetTableID("BI")
	biName := ddlogProgram.GetTableName(biID)
	assert.Equal(t, "BI", biName)

	tableID := ddlogProgram.GetTableID("L0I")
	outTableID := ddlogProgram.GetTableID("OL0I")
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
		int64(1),
	).Return()

	err = ddlogProgram.ApplyUpdatesAsTransaction(cmd)
	assert.Nil(t, err, "Error when processing transaction")

	err = ddlogProgram.Stop()
	assert.Nil(t, err, "Error when stopping DDlog program")

	m.AssertExpectations(t)
}

func benchmarkTransaction(b *testing.B, commitFn func(p *Program) error) {
	outRecordHandler, err := NewOutRecordSink()
	assert.Nil(b, err)
	ddlogProgram, err := NewProgram(numDDlogWorkers, outRecordHandler)
	assert.Nil(b, err, "Error when running DDlog program")

	tableID := ddlogProgram.GetTableID("ZI21")
	constructor := NewCString("ZI21")
	defer constructor.Free()

	b.StartTimer()

	numCommandsPerTransaction := 100
	for i := 0; i < b.N; i++ {
		err = ddlogProgram.StartTransaction()
		assert.Nil(b, err, "Error when starting transaction")
		for j := 0; j < numCommandsPerTransaction; j++ {
			rStruct := NewRecordStructStatic(
				constructor,
				NewRecordU32(uint32(i*numCommandsPerTransaction+j)),
			)
			cmd := NewInsertCommand(tableID, rStruct)
			ddlogProgram.ApplyUpdate(cmd)
		}
		err = commitFn(ddlogProgram)
		assert.Nil(b, err, "Error when committing transaction")
	}

	b.StopTimer()

	err = ddlogProgram.Stop()
	assert.Nil(b, err, "Error when stopping DDlog program")
}

func BenchmarkTransaction(b *testing.B) {
	benchmarkTransaction(b, func(p *Program) error { return p.CommitTransaction() })
}

func BenchmarkTransactionChangesAsArray(b *testing.B) {
	benchmarkTransaction(b, func(p *Program) error { return p.CommitTransactionChangesAsArray() })
}
