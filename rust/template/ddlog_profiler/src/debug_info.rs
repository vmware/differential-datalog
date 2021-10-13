use crate::source_code::SourcePosition;
use crate::Profile;
use std::{borrow::Cow, fmt::Display, slice};

const SHORT_DESCR_LEN: u32 = 96;

/// Different types of operators used to encode DDlog programs to a
/// Differential Dataflow graph.  These are closely tied to the encoding
/// used by the DDlog compiler.
///
/// We use the following naming conventions for `OperatorDebugInfo` constructors:
/// * `rel` - relation name
/// * `arr` - arrangement descriptor represented as a pattern
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperatorDebugInfo {
    /// Root of the operator tree.
    Dataflow,
    /// Operator injected by the DDlog runtime that does not directly correspond to
    /// any user code.
    Builtin { description: Cow<'static, str> },
    /// Feed data to input relation.
    Input {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Apply function to relation.
    Map { source_pos: SourcePosition },
    /// Filter relation.
    Filter { source_pos: SourcePosition },
    /// Filter-map relation.
    FilterMap { source_pos: SourcePosition },
    /// Compute the head of a rule.  Internally, this is just a FlatMap.
    Head { source_pos: SourcePosition },
    /// Flatmap relation.
    Flatmap { source_pos: SourcePosition },
    /// Recursive relation's feedback loop.
    FixedPoint {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Recursive fragment of the dataflow graph.
    RecursiveComponent { rels: Vec<Cow<'static, str>> },
    /// Prepare a stream relation to a streaming transformation, i.e., a stateful
    /// transformation (arrange/group_by) that only applies to changes in the
    /// current timestamp.  This covers `differentiate` and `integrate` operators.
    /// The transformation itself will happen in a separate nested profiling
    /// context.
    StreamXForm { source_pos: SourcePosition },
    /// Join delayed relation with `Enabled` relation.
    JoinWithEnabled {
        rel: Cow<'static, str>,
        used_at: Vec<SourcePosition>,
    },
    /// Consolidate updates to an output relation.
    ConsolidateOutput {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Delay operaor (`Rel-N`).
    DelayedRelation {
        rel: Cow<'static, str>,
        delay: usize,
        used_at: Vec<SourcePosition>,
    },
    /// Inspect output relation, invoking an output callback on each record.
    InspectOutput {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Output relation probe.
    ProbeOutput {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Probe attached to arrangement used to backup index.
    ProbeIndex {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        indexes: Vec<Cow<'static, str>>,
        used_at: Vec<SourcePosition>,
    },
    /// Concatenate all rules for a relation.
    ConcatenateRelation {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Distinct the relation.
    DistinctRelation {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Arrange non-recursive relation.
    ArrangeRelationNonRec {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: Vec<SourcePosition>,
    },
    /// Arrange recursive relation inside recursive scope.
    ArrangeRelationRecInner {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: Vec<SourcePosition>,
    },
    /// Arrange recursive relation in the top-level scope.
    ArrangeRelationRecOuter {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: Vec<SourcePosition>,
    },
    /// DDlog Inspect operator.
    Inspect { source_pos: SourcePosition },
    /// Arrange the relation yielded by a prefix of a rule.
    Arrange {
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Arrange the stream produced by a prefix of a rule.
    StreamArrange {
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Join a prefix of a rule with arrangement `arr` of relation `rel`.
    Join {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Semijoin a prefix of a rule with arrangement `arr` of relation `rel`.
    Semijoin {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Antijoin a prefix of a rule with arrangement `arr` of relation `rel`.
    Antijoin {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Join the arrangement returned by a prefix of a rule with arrangement
    /// `arr` of a stream relation.
    ArrStreamJoin {
        stream: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Semijoin the arrangement returned by a prefix of a rule with
    /// arrangement `arr` of a stream relation.
    ArrStreamSemijoin {
        stream: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Join the stream returned by a prefix of a rule, arranged using `arr1`
    /// with arrangement `arr2` of relation `rel2`.
    StreamArrJoin {
        arr1: Cow<'static, str>,
        rel2: Cow<'static, str>,
        arr2: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Semijoin the stream returned by a prefix of a rule, arranged using `arr1`
    /// with arrangement `arr2` of relation `rel2`.
    StreamArrSemijoin {
        arr1: Cow<'static, str>,
        rel2: Cow<'static, str>,
        arr2: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Filter and group arrangement.  Apply aggregation function to the resulting
    /// group.
    GroupBy { source_pos: SourcePosition },
    /// DDlog's differentiation operator (`rel'`).
    Differentiate {
        rel: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Apply relation transformer.
    ApplyTransformer {
        transformer: Cow<'static, str>,
        source_pos: SourcePosition,
    },
    /// Relation defined in the outer scope enters recursive scope.
    RelEnterRecursiveScope { rel: Cow<'static, str> },
    /// Arrangement defined in the outer scope enters recursive scope.
    ArrEnterRecursiveScope {
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
    },
    /// Export relation defined in the recursive scope to the outer scope.
    RelLeaveRecursiveScope { rel: Cow<'static, str> },
}

macro_rules! help {
    ($text:literal, $name:literal) => {
        concat!(
            "<a target=\"_blank\" href=\"https://github.com/vmware/differential-datalog/wiki/Profiler-Help#",
            $name,
            "\">",
            $text,
            "</a>"
        )
    };
}

impl OperatorDebugInfo {
    pub fn dataflow() -> Self {
        Self::Dataflow
    }

    pub fn builtin(description: Cow<'static, str>) -> Self {
        Self::Builtin { description }
    }

    pub fn input(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::Input { rel, source_pos }
    }

    pub fn filter(source_pos: SourcePosition) -> Self {
        Self::Filter { source_pos }
    }

    pub fn filter_map(source_pos: SourcePosition) -> Self {
        Self::FilterMap { source_pos }
    }

    pub fn head(source_pos: SourcePosition) -> Self {
        Self::Head { source_pos }
    }

    pub fn flatmap(source_pos: SourcePosition) -> Self {
        Self::Flatmap { source_pos }
    }

    pub fn map(source_pos: SourcePosition) -> Self {
        Self::Map { source_pos }
    }

    pub fn fixed_point(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::FixedPoint { rel, source_pos }
    }

    pub fn recursive_component(rels: &[Cow<'static, str>]) -> Self {
        Self::RecursiveComponent {
            rels: rels.to_vec(),
        }
    }

    pub fn stream_xform(source_pos: SourcePosition) -> Self {
        Self::StreamXForm { source_pos }
    }

    pub fn join_with_enabled(rel: Cow<'static, str>, used_at: &[SourcePosition]) -> Self {
        Self::JoinWithEnabled {
            rel,
            used_at: used_at.to_vec(),
        }
    }

    pub fn consolidate_output(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::ConsolidateOutput { rel, source_pos }
    }

    pub fn delayed_rel(rel: Cow<'static, str>, delay: usize, used_at: &[SourcePosition]) -> Self {
        Self::DelayedRelation {
            rel,
            delay,
            used_at: used_at.to_vec(),
        }
    }

    pub fn inspect_output(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::InspectOutput { rel, source_pos }
    }

    pub fn probe_output(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::ProbeOutput { rel, source_pos }
    }

    pub fn probe_index(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        indexes: &[Cow<'static, str>],
        used_at: &[SourcePosition],
    ) -> Self {
        Self::ProbeIndex {
            rel,
            arr,
            indexes: indexes.to_vec(),
            used_at: used_at.to_vec(),
        }
    }

    pub fn concatenate_relation(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::ConcatenateRelation { rel, source_pos }
    }

    pub fn distinct_relation(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::DistinctRelation { rel, source_pos }
    }

    pub fn arrange_relation_non_rec(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: &[SourcePosition],
    ) -> Self {
        Self::ArrangeRelationNonRec {
            rel,
            arr,
            used_at: used_at.to_vec(),
        }
    }

    pub fn arrange_relation_rec_inner(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: &[SourcePosition],
    ) -> Self {
        Self::ArrangeRelationRecInner {
            rel,
            arr,
            used_at: used_at.to_vec(),
        }
    }

    pub fn arrange_relation_rec_outer(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        used_at: &[SourcePosition],
    ) -> Self {
        Self::ArrangeRelationRecOuter {
            rel,
            arr,
            used_at: used_at.to_vec(),
        }
    }

    pub fn inspect(source_pos: SourcePosition) -> Self {
        Self::Inspect { source_pos }
    }

    pub fn arrange(arr: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::Arrange { arr, source_pos }
    }

    pub fn stream_arrange(arr: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::StreamArrange { arr, source_pos }
    }

    pub fn join(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::Join {
            rel,
            arr,
            source_pos,
        }
    }

    pub fn semijoin(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::Semijoin {
            rel,
            arr,
            source_pos,
        }
    }

    pub fn antijoin(
        rel: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::Antijoin {
            rel,
            arr,
            source_pos,
        }
    }

    pub fn stream_arr_join(
        arr1: Cow<'static, str>,
        rel2: Cow<'static, str>,
        arr2: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::StreamArrJoin {
            arr1,
            rel2,
            arr2,
            source_pos,
        }
    }

    pub fn stream_arr_semijoin(
        arr1: Cow<'static, str>,
        rel2: Cow<'static, str>,
        arr2: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::StreamArrSemijoin {
            arr1,
            rel2,
            arr2,
            source_pos,
        }
    }

    pub fn arr_stream_join(
        stream: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::ArrStreamJoin {
            stream,
            arr,
            source_pos,
        }
    }

    pub fn arr_stream_semijoin(
        stream: Cow<'static, str>,
        arr: Cow<'static, str>,
        source_pos: SourcePosition,
    ) -> Self {
        Self::ArrStreamSemijoin {
            stream,
            arr,
            source_pos,
        }
    }

    pub fn group_by(source_pos: SourcePosition) -> Self {
        Self::GroupBy { source_pos }
    }

    pub fn differentiate(rel: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::Differentiate { rel, source_pos }
    }

    pub fn apply_transformer(transformer: Cow<'static, str>, source_pos: SourcePosition) -> Self {
        Self::ApplyTransformer {
            transformer,
            source_pos,
        }
    }

    pub fn arr_enter_recursive_scope(rel: Cow<'static, str>, arr: Cow<'static, str>) -> Self {
        Self::ArrEnterRecursiveScope { rel, arr }
    }

    pub fn rel_enter_recursive_scope(rel: Cow<'static, str>) -> Self {
        Self::RelEnterRecursiveScope { rel }
    }

    pub fn rel_leave_recursive_scope(rel: Cow<'static, str>) -> Self {
        Self::RelLeaveRecursiveScope { rel }
    }

    fn code<S>(s: &S) -> String
    where
        S: Display,
    {
        format!("<span class=\"code\">{}</span>", s)
    }

    /// Short description that fits in a cell of a table.
    pub fn short_descr(&self, profile: &Profile) -> String {
        match self {
            Self::Dataflow => help!("Dataflow", "dataflow").to_string(),
            Self::Builtin { description } => format!(
                "{}&nbsp;{}",
                help!("Builtin operator", "builtin"),
                Self::code(description)
            ),
            Self::Input { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Ingest relation", "ingest"),
                Self::code(rel)
            ),
            Self::Filter { source_pos } => format!(
                "{}&nbsp;{}",
                help!("Filter", "filter"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::Map { source_pos } => format!(
                "{}&nbsp;{}",
                help!("Map", "map"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::FilterMap { source_pos } => format!(
                "{}&nbsp;{}",
                help!("FilterMap", "filter_map"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::Head { source_pos } => format!(
                "{}&nbsp;{}",
                help!("Eval rule head", "head"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::Flatmap { source_pos } => format!(
                "{}&nbsp;{}",
                help!("FlatMap", "flatmap"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::FixedPoint { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Fixed point of recursive relation", "fixed_point"),
                Self::code(rel)
            ),
            Self::RecursiveComponent { .. } => {
                help!("Recursive program fragment", "recursive_fragment").to_string()
            }
            Self::StreamXForm { .. } => help!("Stream transform", "stream_transform").to_string(),
            Self::JoinWithEnabled { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Enable delayed relation", "enable"),
                Self::code(rel)
            ),
            Self::ConsolidateOutput { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Consolidate output relation", "consolidate"),
                Self::code(rel)
            ),
            Self::DelayedRelation { rel, delay, .. } => format!(
                "{}&nbsp;{}",
                help!("Delay", "delay"),
                Self::code(&format!("{}-{}", rel, delay))
            ),
            Self::InspectOutput { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Inspect output relation", "inspect_output"),
                Self::code(rel)
            ),
            Self::ProbeOutput { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Probe output relation", "probe_output"),
                Self::code(rel)
            ),
            Self::ProbeIndex { indexes, .. } if indexes.len() == 1 => format!(
                "{}&nbsp;{}",
                help!("Probe index", "probe_index"),
                Self::code(&indexes[0])
            ),
            Self::ProbeIndex { indexes, .. } => format!(
                "{}&nbsp;{}",
                help!("Probe indexes", "probe_index"),
                Self::code(&indexes.as_slice().join(", "))
            ),
            Self::ConcatenateRelation { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Concatenate rules for relation", "concatenate_relation"),
                Self::code(rel)
            ),
            Self::DistinctRelation { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Deduplicate relation", "distinct"),
                Self::code(rel)
            ),
            Self::ArrangeRelationNonRec { rel, .. } => {
                format!("{}&nbsp;{}", help!("Arrange", "arrange"), Self::code(rel))
            }
            Self::ArrangeRelationRecInner { rel, .. } => {
                format!("{}&nbsp;{}", help!("Arrange", "arrange"), Self::code(rel))
            }
            Self::ArrangeRelationRecOuter { rel, .. } => {
                format!("{}&nbsp;{}", help!("Arrange", "arrange"), Self::code(rel))
            }
            Self::Inspect { source_pos } => format!(
                "{}&nbsp;{}",
                help!("Inspect operator", "inspect"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::Arrange { arr, .. } => format!(
                "{}&nbsp;{}",
                help!("Arrange rule prefix by", "arrange_prefix"),
                Self::code(arr)
            ),
            Self::StreamArrange { arr, .. } => format!(
                "{}&nbsp;{}",
                help!("Arrange streaming rule prefix by", "stream_arrange"),
                Self::code(arr)
            ),
            Self::Join { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Join rule prefix with", "join"),
                Self::code(rel)
            ),
            Self::Semijoin { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Semijoin rule prefix with", "semijoin"),
                Self::code(rel)
            ),
            Self::Antijoin { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Antijoin rule prefix with", "antijoin"),
                Self::code(rel)
            ),
            Self::ArrStreamJoin { stream, .. } => format!(
                "{}&nbsp;{}",
                help!("Join rule prefix with stream", "arr_stream_join"),
                Self::code(stream)
            ),
            Self::ArrStreamSemijoin { stream, .. } => format!(
                "{}&nbsp;{}",
                help!("Semijoin rule prefix with stream", "arr_stream_semijoin"),
                Self::code(stream)
            ),
            Self::StreamArrJoin { rel2, .. } => format!(
                "{}&nbsp;{}",
                help!("Join streaming rule prefix with", "stream_arr_join"),
                Self::code(rel2)
            ),
            Self::StreamArrSemijoin { rel2, .. } => format!(
                "{}&nbsp;{}",
                help!("Semijoin streaming rule prefix with", "stream_arr_semijoin"),
                Self::code(rel2)
            ),
            Self::GroupBy { source_pos } => format!(
                "{}&nbsp;{}",
                help!("Group-by operator", "group_by"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::Differentiate { rel, .. } => format!(
                "{}&nbsp;{}",
                help!("Differentiate", "differentiate"),
                Self::code(rel)
            ),
            Self::ApplyTransformer { source_pos, .. } => format!(
                "{}&nbsp;{}",
                help!("Relation transformer", "transformer"),
                Self::code(&profile.snippet(source_pos, SHORT_DESCR_LEN))
            ),
            Self::RelEnterRecursiveScope { rel } => format!(
                "Relation&nbsp;{}&nbsp;{}&nbsp;recursive scope",
                Self::code(rel),
                help!("enters", "rel_enter")
            ),
            Self::ArrEnterRecursiveScope { rel, .. } => format!(
                "Arranged relation&nbsp;{}&nbsp;{}&nbsp;recursive scope",
                Self::code(rel),
                help!("enters", "arr_enter")
            ),
            Self::RelLeaveRecursiveScope { rel } => format!(
                "Relation&nbsp;{}&nbsp;{}&nbsp;recursive scope",
                Self::code(rel),
                help!("leaves", "leave")
            ),
        }
    }

    /// More detailed description.
    pub fn description(&self) -> String {
        match self {
            Self::Dataflow => "".to_string(),
            Self::Builtin { .. } => "".to_string(),
            Self::Input { .. } => "".to_string(),
            Self::Filter { .. } => "".to_string(),
            Self::Map { .. } => "".to_string(),
            Self::FilterMap { .. } => "".to_string(),
            Self::Head { .. } => "".to_string(),
            Self::Flatmap { .. } => "".to_string(),
            Self::FixedPoint { .. } => "".to_string(),
            Self::RecursiveComponent { rels, .. } => format!("Recursive program fragment consisting of relations&nbsp;{}", Self::code(&rels.as_slice().join(", "))),
            Self::StreamXForm { .. } => "".to_string(),
            Self::JoinWithEnabled { .. } => "".to_string(),
            Self::ConsolidateOutput { .. } => "".to_string(),
            Self::DelayedRelation { .. } => "".to_string(),
            Self::InspectOutput { .. } => "".to_string(),
            Self::ProbeOutput { .. } => "".to_string(),
            Self::ProbeIndex { rel, arr, indexes, .. } if indexes.len() == 1 => format!("Probe arrangement of&nbsp;{}&nbsp;by&nbsp;{}&nbsp;used to construct index&nbsp;{}", Self::code(rel), Self::code(arr), Self::code(&indexes[0])),
            Self::ProbeIndex { rel, arr, indexes, .. } => format!("Probe arrangement of&nbsp;{}&nbsp;by&nbsp;{}&nbsp;used to construct indexes&nbsp;{}", Self::code(rel), Self::code(arr), Self::code(&indexes.as_slice().join(", "))),
            Self::ConcatenateRelation { .. } => "".to_string(),
            Self::DistinctRelation { .. } => "".to_string(),
            Self::ArrangeRelationNonRec { rel, arr, .. } => format!("Arrange relation&nbsp;{}&nbsp;using pattern&nbsp;{}", Self::code(rel), Self::code(arr)),
            Self::ArrangeRelationRecInner { rel, arr, .. } => format!("Arrange relation&nbsp;{}&nbsp;using pattern&nbsp;{}&nbsp;inside recursive scope", Self::code(rel), Self::code(arr)),
            Self::ArrangeRelationRecOuter { rel, arr, .. } => format!("Arrange relation&nbsp;{}&nbsp;using pattern&nbsp;{}", Self::code(rel), Self::code(arr)),
            Self::Inspect { .. } => "".to_string(),
            Self::Arrange { .. } => "".to_string(),
            Self::StreamArrange { .. } => "".to_string(),
            Self::Join { rel, arr, .. } => format!("Join rule prefix with relation&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(rel), Self::code(arr)),
            Self::Semijoin { rel, arr, .. } => format!("Semijoin rule prefix with relation&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(rel), Self::code(arr)),
            Self::Antijoin { rel, arr, .. } => format!("Antijon rule prefix with relation&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(rel), Self::code(arr)),
            Self::ArrStreamJoin { stream, arr, .. } => format!("Join rule prefix with stream&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(stream), Self::code(arr)),
            Self::ArrStreamSemijoin { stream, arr, .. } => format!("Semijoin rule prefix with stream&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(stream), Self::code(arr)),
            Self::StreamArrJoin { arr1, rel2, arr2, .. } => format!("Join streaming rule prefix arranged by&nbsp;{}&nbsp;with relation&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(arr1), Self::code(rel2), Self::code(arr2)),
            Self::StreamArrSemijoin { arr1, rel2, arr2, .. } => format!("Semijoin streaming rule prefix arranged by&nbsp;{}&nbsp;with relation&nbsp;{}&nbsp;arranged using pattern&nbsp;{}", Self::code(arr1), Self::code(rel2), Self::code(arr2)),
            Self::GroupBy { .. } => "".to_string(),
            Self::Differentiate { .. } => "".to_string(),
            Self::ApplyTransformer { .. } => "".to_string(),
            Self::RelEnterRecursiveScope { .. } => "".to_string(),
            Self::ArrEnterRecursiveScope { .. } => "".to_string(),
            Self::RelLeaveRecursiveScope { .. } => "".to_string(),
        }
    }

    pub fn source_pos(&self) -> &[SourcePosition] {
        match self {
            Self::Dataflow => &[],
            Self::Builtin { .. } => &[],
            Self::Input { source_pos, .. } => slice::from_ref(source_pos),
            Self::Filter { source_pos } => slice::from_ref(source_pos),
            Self::FilterMap { source_pos } => slice::from_ref(source_pos),
            Self::Head { source_pos } => slice::from_ref(source_pos),
            Self::Flatmap { source_pos } => slice::from_ref(source_pos),
            Self::Map { source_pos } => slice::from_ref(source_pos),
            Self::FixedPoint { source_pos, .. } => slice::from_ref(source_pos),
            Self::RecursiveComponent { .. } => &[],
            Self::StreamXForm { source_pos } => slice::from_ref(source_pos),
            Self::JoinWithEnabled { used_at, .. } => used_at,
            Self::ConsolidateOutput { source_pos, .. } => slice::from_ref(source_pos),
            Self::DelayedRelation { used_at, .. } => used_at,
            Self::InspectOutput { source_pos, .. } => slice::from_ref(source_pos),
            Self::ProbeIndex { used_at, .. } => used_at,
            Self::ProbeOutput { source_pos, .. } => slice::from_ref(source_pos),
            Self::ConcatenateRelation { source_pos, .. } => slice::from_ref(source_pos),
            Self::DistinctRelation { source_pos, .. } => slice::from_ref(source_pos),
            Self::ArrangeRelationNonRec { used_at, .. } => used_at,
            Self::ArrangeRelationRecInner { used_at, .. } => used_at,
            Self::ArrangeRelationRecOuter { used_at, .. } => used_at,
            Self::Inspect { source_pos } => slice::from_ref(source_pos),
            Self::Arrange { source_pos, .. } => slice::from_ref(source_pos),
            Self::StreamArrange { source_pos, .. } => slice::from_ref(source_pos),
            Self::Join { source_pos, .. } => slice::from_ref(source_pos),
            Self::Semijoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::Antijoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::ArrStreamJoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::ArrStreamSemijoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::StreamArrJoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::StreamArrSemijoin { source_pos, .. } => slice::from_ref(source_pos),
            Self::GroupBy { source_pos } => slice::from_ref(source_pos),
            Self::Differentiate { source_pos, .. } => slice::from_ref(source_pos),
            Self::ApplyTransformer { source_pos, .. } => slice::from_ref(source_pos),
            Self::RelEnterRecursiveScope { .. } => &[],
            Self::ArrEnterRecursiveScope { .. } => &[],
            Self::RelLeaveRecursiveScope { .. } => &[],
        }
    }
}

/// Debug info for a DDlog operator.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct RuleDebugInfo {
    /// Source code location of the rule.
    source_pos: SourcePosition,
}

impl RuleDebugInfo {
    pub fn new(source_pos: SourcePosition) -> Self {
        Self { source_pos }
    }
}

/// Debug info for an arrangement.
#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct ArrangementDebugInfo {
    /// Arrangement pattern.
    pub arrange_by: Cow<'static, str>,
    /// Code fragments that explain why this arrangement is needed.
    /// Points to occurrences of the relation in a rule or index.
    pub used_at: Vec<SourcePosition>,
    /// Names of indexes backs by this arrangement.
    pub used_in_indexes: Vec<Cow<'static, str>>,
}

impl ArrangementDebugInfo {
    pub fn new(
        arrange_by: Cow<'static, str>,
        used_at: &[SourcePosition],
        used_in_indexes: &[Cow<'static, str>],
    ) -> Self {
        Self {
            arrange_by,
            used_at: used_at.to_vec(),
            used_in_indexes: used_in_indexes.to_vec(),
        }
    }
}
