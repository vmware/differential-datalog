use serde::ser::*;
use serde::de::*;
use abomonation::Abomonation;
use std::hash::Hash;
use std::fmt::Debug;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashSet;

use differential_dataflow::input::InputSession;

/* Value trait describes types that can be stored in a collection */
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + 'static {}

type RelId = usize;
type ArrId = (usize, usize);

/* Datalog program is a vector of strongly connected components (representing mutually recursive
 * rules) or individual non-recursive relations. */
pub struct Program<V: Val> {
    pub relations: Vec<Relation<V>>,
    pub nodes: Vec<ProgNode>
}

pub enum ProgNode {
    SCC{scc: Vec<RelId>},
    Relation{relation: RelId}
}

/* Relation defines a set of rules and a set of arrangements with which this relation is used in 
 * rules.  The set of rules can be empty (if this is a ground relation); the set of arrangements 
 * can also be empty if the relation is not used in the RHS of any rules */
pub struct Relation<V: Val> {
    name:         String,
    rules:        Vec<Rule<V>>,
    arrangements: Vec<Arrangement<V>>
}

/* Function used to map a collection of values to another collection of values */
pub type MapFunc<V>        = fn(&V) -> &V;

/* Function used to filter a collection of values */
pub type FilterFunc<V>     = fn(&V) -> bool;

/* Function that simultaneously filters and maps a collection */
pub type FilterMapFunc<V>  = fn(&V) -> Option<&V>;

/* Function that arranges a collection, possibly filtering it  */
pub type ArrangeFunc<V> = fn(&V) -> Option<(&V,&V)>;

/* Function that assembles the result of a join to a new value */
pub type JoinFunc<V> = fn(&V,&V,&V) -> V;

pub struct Rule<V: Val> {
    rel:    RelId,        // first relation in the body of the rule
    xforms: Vec<XForm<V>> // chain of transformations
}

pub enum XForm<V: Val> {
    Map {
        mfun: MapFunc<V>
    },
    Filter {
        ffun: FilterFunc<V>
    },
    FilterMap {
        fmfun: FilterMapFunc<V>
    },
    Join {
        afun: ArrangeFunc<V>, // arrange the relation before performing join on it
        arrangement: ArrId,   // arrangement to join with
        jfun: JoinFunc<V>     // put together ouput value
    },
    Antijoin {
        afun: ArrangeFunc<V>, // arrange the relation before performing antijoin
        arrangement: ArrId    // arrangement to antijoin with
    }
}

pub struct Arrangement<V: Val> {
    name: String,
    afun: ArrangeFunc<V>
}


pub struct RelationInstance<V: Val> {
    input:    InputSession<u64, V, isize>,
    delta:    Rc<RefCell<HashSet<V>>>,
    elements: Rc<RefCell<HashMap<V, i8>>>
}

/* Running datalog program.
 */
pub struct RunningProgram<V: Val> {
    relations: Vec<RelationInstance<V>>
}

impl<V:Val> Program<V> {
    pub fn run(&self) -> RunningProgram<V> {
        let mut rels = Vec::new();

        RunningProgram{relations: rels}
    }
}


