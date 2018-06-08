use serde::ser::*;
use serde::de::*;
use abomonation::Abomonation;
use std::hash::Hash;
use std::fmt::Debug;
use std::collections::HashMap;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashSet;

use timely;
use timely_communication::initialize::Configuration;
use timely_communication::Allocator;
use differential_dataflow::input::{Input,InputSession};
use differential_dataflow::operators::*;
use differential_dataflow::Collection;
use timely::dataflow::scopes::*;


const NTIMELY_THREADS: usize = 1;

/* Value trait describes types that can be stored in a collection */
pub trait Val: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + 'static {}
impl<T> Val for T where T: Eq + Ord + Clone + Send + Hash + PartialEq + PartialOrd + Serialize + DeserializeOwned + Debug + Abomonation + 'static {}

type RelId = usize;
type ArrId = (usize, usize);

/* Datalog program is a vector of strongly connected components (representing mutually recursive
 * rules) or individual non-recursive relations. */
#[derive(Clone)]
pub struct Program<V: Val> {
    pub nodes: Vec<ProgNode<V>>
}

#[derive(Clone)]
pub enum ProgNode<V: Val> {
    RelNode{rel: Relation<V>},
    SCCNode{rels: Vec<Relation<V>>}
}

/* Relation defines a set of rules and a set of arrangements with which this relation is used in 
 * rules.  The set of rules can be empty (if this is a ground relation); the set of arrangements 
 * can also be empty if the relation is not used in the RHS of any rules.  name is the
 * human-readable name of the relation; scc is the index of the strongly connected component of 
 * the dependency graph that the relation belongs to. */
#[derive(Clone)]
pub struct Relation<V: Val> {
    name:         String,
    id:           RelId,      
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

#[derive(Clone)]
pub struct Rule<V: Val> {
    rel:    RelId,        // first relation in the body of the rule
    xforms: Vec<XForm<V>> // chain of transformations
}

#[derive(Clone)]
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

#[derive(Clone)]
pub struct Arrangement<V: Val> {
    name: String,
    afun: ArrangeFunc<V>
}


/* Running datalog program.
 */
pub struct RunningProgram<V: Val> {
    relations: HashMap<RelId, RelationInstance<V>>
}

pub type ValSet<V> = Rc<RefCell<HashSet<V>>>;
pub type DeltaSet<V> = Rc<RefCell<HashMap<V, i8>>>;
type ValCollection<'a, V> = Collection<Child<'a, Root<Allocator>, u64>, V, isize>;

/* Runtime representation of relation
 */
pub struct RelationInstance<V: Val> {
    /* Input session to feed data to relation */
    input:    InputSession<u64, V, isize>,
    /* Set of all elements in the relation */
    elements: ValSet<V>,
    /* Changes since start of transaction */
    delta:    DeltaSet<V>
}

impl<V:Val> Program<V> {
    pub fn run(&self) -> RunningProgram<V> {
        let mut rels = HashMap::new();

        let mut elements : HashMap<RelId, ValSet<V>>   = HashMap::new();
        let mut deltas   : HashMap<RelId, DeltaSet<V>> = HashMap::new();

        for node in self.nodes.iter() {
            match node {
                ProgNode::RelNode{rel: r} => {
                    elements.insert(r.id, Rc::new(RefCell::new(HashSet::new())));
                    deltas.insert(r.id, Rc::new(RefCell::new(HashMap::new())));
                },
                ProgNode::SCCNode{rels: rs} => {
                    for r in rs.iter() {
                        elements.insert(r.id, Rc::new(RefCell::new(HashSet::new())));
                        deltas.insert(r.id, Rc::new(RefCell::new(HashMap::new())));
                    }
                }
            }
        };
        /* Clone the program, so that it can be accessed inside the timely computation */
        let prog = self.clone();

        // start up timely computation
        timely::execute(Configuration::Process(NTIMELY_THREADS), move |worker: &mut Root<Allocator>| {
            let sessions = worker.dataflow::<u64,_,_>(|outer: &mut Child<Root<Allocator>, u64>| {
                let mut sessions : HashMap<RelId, InputSession<u64, V, isize>> = HashMap::new();
                let mut collections : HashMap<RelId, ValCollection<V>> = HashMap::new();
                //let mut arrangements = HashMap::new();
                for node in &prog.nodes {
                    match node {
                        ProgNode::RelNode{rel:r} => {
                            let (session, mut collection) = outer.new_collection::<V,isize>();
                            /* apply rules */
                            for rule in &r.rules {
                                collection = add_rule(&collection, rule, &collections);
                            };
                            collection = collection.distinct();
                            /* generate arrangements */
                            sessions.insert(r.id, session);
                            collections.insert(r.id, collection);
                        },
                        _ => {
                            /* create a nested scope to represent recursive relations */
                            panic!("recursive relations are not implemented");
                        }
                    };
                };
                sessions
            });
        });

        RunningProgram{relations: rels}
    }
}

fn add_rule<'a, V:Val>(collection: &ValCollection<'a,V>, 
                       rule: &Rule<V>,
                       collections: &HashMap<RelId, ValCollection<'a,V>>) -> ValCollection<'a,V> {
    match collections.get(&rule.rel) {
        Some(c) => collection.concat(c),
        None    => panic!("mk_rule: unknown relation id {}", rule.rel)
    }
}
