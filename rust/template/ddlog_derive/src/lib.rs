use syn::{parse_macro_input, DeriveInput, GenericParam, Generics, TypeParamBound};

mod from_record;

/// Allows deriving `FromRecord` for structs and enums
///
/// The struct/enum and individual fields or enum variants can be renamed with the
/// `#[ddlog(rename = "new name")]` or `#[ddlog(from_record = "new name")]` attributes
///
/// ```rust
/// # use ddlog_derive::FromRecord;
///
/// #[derive(FromRecord)]
/// #[ddlog(rename = "foo")]
/// struct Foo {
///     #[ddlog(rename = "baz")]
///     bar: usize,
/// }
/// ```
///
#[proc_macro_derive(FromRecord, attributes(ddlog, from_record))]
pub fn derive_from_record(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    from_record::from_record_inner(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

/// Add a trait bound to every generic, skipping the addition if the generic
/// already has the required trait bound
fn add_trait_bounds(mut generics: Generics, bounds: Vec<TypeParamBound>) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            for bound in bounds.iter() {
                if !type_param
                    .bounds
                    .iter()
                    .any(|type_bound| type_bound == bound)
                {
                    type_param.bounds.push(bound.clone());
                }
            }
        }
    }

    generics
}
