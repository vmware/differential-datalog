// TODO: The expression and relation optimization languages may
//       have to be unified

use egg::{rewrite, Analysis, Id};

type EGraph = egg::EGraph<Relation, RelationAnalysis>;
type Rewrite = egg::Rewrite<Relation, RelationAnalysis>;

egg::define_language! {
    pub enum Relation {
        "join" = Join([Id; 2]),
        "map" = Map([Id; 2]),
        "filter" = Filter([Id; 2]),
        "filter-map" = FilterMap([Id; 2]),
        "lam" = Lambda,
        "app" = Apply([Id; 2]),
        "relation" = Relation,
        "if" = If([Id; 3]),
        "Some" = Some(Id),
        "None" = None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct RelationAnalysis;

impl Analysis<Relation> for RelationAnalysis {
    type Data = ();

    fn merge(&self, _to: &mut Self::Data, _from: Self::Data) -> bool {
        false
    }

    fn make(_egraph: &EGraph, _enode: &Relation) -> Self::Data {}
}

fn rules() -> Vec<Rewrite> {
    vec![
        rewrite!(
            "collapse-map";
            "(map (map ?rel ?map1) ?map2)"
                => "(map ?rel (app ?map1 ?map2))"
        ),
        rewrite!(
            "collapse-filter";
            "(filter (filter ?rel ?filter1) ?filter2)"
                => "(filter ?rel (app ?filter1 ?filter2))"
        ),
        rewrite!(
            "filter-then-map";
            "(map (filter ?rel ?filter) ?map)"
                => "(filter-map ?rel (if ?filter (Some ?map) None))"
        ),
    ]
}

// Hack because `egg::test_fn` uses `env_logger` for some reason
#[cfg(test)]
mod env_logger {
    pub fn builder() -> Foo {
        Foo
    }

    pub struct Foo;

    impl Foo {
        pub fn is_test(&self, _: bool) -> &Self {
            self
        }

        pub fn try_init(&self) {}
    }
}

egg::test_fn! {
    collapse_maps,
    rules(),
    "(map (map (map relation lam) lam) lam)"
    =>
    "(map ?rel (app lam (app lam lam)))"
}

egg::test_fn! {
    collapse_filters,
    rules(),
    "(filter (filter (filter relation lam) lam) lam)"
    =>
    "(filter ?rel (app lam (app lam lam)))"
}

egg::test_fn! {
    filter_and_map_to_filter_map,
    rules(),
    "(map (map (filter relation lam) lam) lam)"
    =>
    "(filter-map ?rel (if lam (Some (app lam lam)) None))"
}
