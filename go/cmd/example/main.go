package main

import (
	"flag"

	"k8s.io/klog"

	"github.com/vmware/differential-datalog/go/pkg/ddlog"
)

// This example program assumes that the DDlog program being used is test/types_test/typesTest.dl.

const numDDlogWorkers = 1

func logger(msg string) {
	klog.Errorf(msg)
}

// Example is the Go type corresponding to ExampleRelation in our DDlog program.
type Example struct {
	Name   string
	Values []uint32
	Labels map[string]string
}

var (
	exampleRelationConstructor = ddlog.NewCString("ExampleRelation")
)

// NewRecordExample transforms a Go Example instance into a DDlog record for ExampleRelation.
func NewRecordExample(example *Example) ddlog.Record {
	rName := ddlog.NewRecordString(example.Name)
	rValues := ddlog.NewRecordVector()
	for _, v := range example.Values {
		rValues.Push(ddlog.NewRecordU32(v))
	}
	rLabels := ddlog.NewRecordMap()
	for k, v := range example.Labels {
		rLabels.Push(ddlog.NewRecordString(k), ddlog.NewRecordString(v))
	}
	return ddlog.NewRecordStructStatic(exampleRelationConstructor, rName, rValues, rLabels)
}

// NewRecordExampleKey returns a DDlog record for the primary key of the provided example. This can
// be used to efficiently delete a previously-inserted record (i.e. without having to provide the
// entire record).
func NewRecordExampleKey(example *Example) ddlog.Record {
	return ddlog.NewRecordString(example.Name)
}

func main() {
	klog.InitFlags(nil)

	recordCommands := flag.String("record-commands", "", "Provide a file name where to record commands sent to DDlog")
	dumpChanges := flag.String("dump-changes", "", "Provide a file name where to dump record changes")
	flag.Parse()

	// Ensures that DDlog will use our own logger (klog) to print error messages.
	ddlog.SetErrMsgPrinter(logger)

	var outRecordHandler ddlog.OutRecordHandler
	if *dumpChanges == "" {
		// ignore changes
		outRecordHandler, _ = ddlog.NewOutRecordSink()
	} else {
		// write output changes to file
		outRecordHandler, _ = ddlog.NewOutRecordDumper(*dumpChanges)
	}

	klog.Infof("Running new DDlog program")
	ddlogProgram, err := ddlog.NewProgram(numDDlogWorkers, outRecordHandler)
	if err != nil {
		klog.Fatalf("Error when creating DDlog program: %v", err)
	}
	defer func() {
		klog.Infof("Stopping DDlog program")
		if err := ddlogProgram.Stop(); err != nil {
			klog.Errorf("Error when stopping DDlog program: %v", err)
		}
	}()

	if *recordCommands != "" {
		ddlogProgram.StartRecordingCommands(*recordCommands)
	}

	e1 := &Example{
		Name:   "MyExample",
		Values: []uint32{4, 1, 777},
		Labels: map[string]string{
			"owner":    "X",
			"priority": "Y",
		},
	}
	r1 := NewRecordExample(e1)

	klog.Infof("Inserting record %s", r1.Dump())
	cmdInsert1 := ddlog.NewInsertCommand(ddlogProgram.GetTableID("ExampleRelation"), r1)
	// In practice, each transction would likely include more than one command.
	if err := ddlogProgram.ApplyUpdatesAsTransaction(cmdInsert1); err != nil {
		klog.Errorf("Error during transaction: %v", err)
	}

	k1 := NewRecordExampleKey(e1)
	klog.Infof("Deleting record with key %s", k1.Dump())
	cmdDelete1 := ddlog.NewDeleteKeyCommand(ddlogProgram.GetTableID("ExampleRelation"), k1)
	if err := ddlogProgram.ApplyUpdatesAsTransaction(cmdDelete1); err != nil {
		klog.Errorf("Error during transaction: %v", err)
	}
}
