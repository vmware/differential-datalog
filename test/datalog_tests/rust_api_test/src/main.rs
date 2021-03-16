use std::borrow::Cow;

// The main auto-generated crate `<progname>_ddlog` (`tutorial_ddlog`
// in this case) declares `HDDlog` type that serves as a reference to a
// running DDlog program.
// `HDDlog` implements `trait differential_datalog::DDlog` (see below).
use tutorial_ddlog::api::HDDlog;

// `enum Relations` enumerates program relations
use tutorial_ddlog::Relations;

// The crate contains several functions that convert between numeric
// relation id's and symbolic names.
use tutorial_ddlog::relid2name;

// Type and function definitions.
use tutorial_ddlog::typedefs::*;

// The differential_datalog crate contains the DDlog runtime that is
// the same for all DDlog programs and simply gets copied to each generated
// DDlog workspace unmodified (this will change in future releases).
use differential_datalog::DeltaMap;
use differential_datalog::{DDlog, DDlogDynamic, DDlogInventory}; // Trait that must be implemented by an instance of a DDlog program. // Type that represents a set of changes to DDlog relations.
                                                                 // Returned by `DDlog::transaction_commit_dump_changes()`.
use differential_datalog::ddval::DDValConvert;
use differential_datalog::ddval::DDValue; // Generic type that wraps all DDlog value. // Trait to convert Rust types to/from DDValue.
                                          // All types used in input and output relations, indexes, and
                                          // primary keys implement this trait.
use differential_datalog::program::RelId; // Numeric relations id.
use differential_datalog::program::Update; // Type-safe representation of a DDlog command (insert/delete_val/delete_key/...)

// The `record` module defines dynamically typed representation of DDlog values and commands.
use differential_datalog::record::Record; // Dynamically typed representation of DDlog values.
use differential_datalog::record::RelIdentifier; // Relation identifier: either `RelId` or `Cow<str>`g.
use differential_datalog::record::UpdCmd; // Dynamically typed representation of DDlog command.

fn main() -> Result<(), String> {
    // Instantiate a DDlog program.
    // Returns a handle to the program and initial contents of output relations.
    // Arguments
    // - number of worker threads (you typically want 1 or 2).
    // - Boolean flag that indicates whether DDlog will track the complete snapshot
    //   of output relations.  Should only be used for debugging in order to dump
    //   the contents of output tables using `HDDlog::dump_table()`.  Otherwise,
    //   indexes are the preferred way to achieve this.
    let (hddlog, init_state) = HDDlog::run(1, false)?;

    println!("Initial state");
    dump_delta(&init_state);

    /*
     * We perform two transactions that insert in the following two DDlog relations
     * (see `tutorial.dl`):
     *
     * ```
     * input relation Word1(word: string, cat: Category)
     * input relation Word2(word: string, cat: Category)
     * ```
     *
     * The first transactio uses the type-safe API, which should be preferred when
     * writing a client bound to a specific known DDlog program.
     *
     * The second transaction uses the dynamically typed record API.
     */

    // There can be at most one transaction at a time.  Attempt to start another transaction
    // when there is one in execution will return an error.
    hddlog.transaction_start()?;

    // A transaction can consist of multiple `apply_updates()` calls, each taking
    // multiple updates.  An update inserts, deletes or modifies a record in a DDlog
    // relation.
    let updates = vec![
        Update::Insert {
            // We are going to insert..
            relid: Relations::Word1 as RelId, // .. into relation with this Id.
            // `Word1` type, declared in the `types` crate has the same fields as
            // the corresponding DDlog type.
            v: Word1 {
                word: "foo-".to_string(),
                cat: Category::CategoryOther,
            }
            .into_ddvalue(),
        },
        Update::Insert {
            relid: Relations::Word2 as RelId,
            v: Word2 {
                word: "bar".to_string(),
                cat: Category::CategoryOther,
            }
            .into_ddvalue(),
        },
    ];
    hddlog.apply_updates(&mut updates.into_iter())?;

    // Commit the transaction; returns a `DeltaMap` object that contains the set
    // of changes to output relations produced by the transaction.
    let mut delta = hddlog.transaction_commit_dump_changes()?;
    //assert_eq!(delta, delta_expected);

    println!("\nState after transaction 1");
    dump_delta(&delta);

    // This shows how to extract values from `DeltaMap`.
    println!("\nEnumerating new phrases");

    // Retrieve the set of changes for a particular relation.
    let new_phrases = delta.get_rel(Relations::Phrases as RelId);
    for (val, weight) in new_phrases.iter() {
        // weight = 1 - insert.
        // weight = -1 - delete.
        assert_eq!(*weight, 1);
        let phrase: &Phrases = Phrases::from_ddvalue_ref(val);
        println!("New phrase: {}", phrase.phrase);
    }

    hddlog.transaction_start()?;

    // `Record` type

    let relid_word1 = hddlog.get_table_id("Word1").unwrap() as RelId;

    // `UpdCmd` is a dynamically typed representation of a DDlog command.
    // It takes a vector or `Record`'s, which represent dynamically typed
    // DDlog values.
    let commands = vec![UpdCmd::Insert(
        RelIdentifier::RelId(relid_word1),
        Record::PosStruct(
            // Positional struct consists of constructor name
            // and a vector of arguments whose number and
            // types must match those of the DDlog constructor.
            // The alternative is `NamedStruct` where arguments
            // are represented as (name, value) pairs.
            Cow::from("Word1"), // Constructor name.
            // Constructor arguments.
            vec![
                Record::String("buzz".to_string()),
                Record::PosStruct(Cow::from("CategoryOther"), vec![]),
            ],
        ),
    )];

    // Use `apply_updates_dynamic` instead of `apply_updates` for dynamically
    // typed commands.
    // This will fail if the records in `commands` don't match the DDlog type
    // declarations (e.g., missing constructor arguments, string instead of integer, etc.)
    hddlog.apply_updates_dynamic(&mut commands.into_iter())?;

    let delta = hddlog.transaction_commit_dump_changes()?;

    println!("\nState after transaction 2");
    dump_delta(&delta);

    hddlog.stop().unwrap();
    Ok(())
}

fn dump_delta(delta: &DeltaMap<DDValue>) {
    for (rel, changes) in delta.iter() {
        println!("Changes to relation {}", relid2name(*rel).unwrap());
        for (val, weight) in changes.iter() {
            println!("{} {:+}", val, weight);
        }
    }
}
