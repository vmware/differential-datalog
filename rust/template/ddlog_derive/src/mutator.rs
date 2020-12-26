use super::{add_trait_bounds, get_rename};
use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse_quote, spanned::Spanned, Data, DataEnum, DataStruct, DeriveInput, Error, Fields,
    FieldsNamed, FieldsUnnamed, Ident, ImplGenerics, Index, Result, TypeGenerics, WhereClause,
};

pub fn mutator_inner(mut input: DeriveInput) -> Result<TokenStream> {
    // The name of the struct
    let struct_ident = input.ident;

    // Use the given rename provided by `#[ddlog(rename = "...")]` or `#[ddlog(into_record = "...")]`
    // as the name of the record, defaulting to the struct's ident if none is given
    let struct_record_name = get_rename("Mutator", "into_record", input.attrs.iter())?
        .unwrap_or_else(|| struct_ident.to_string());

    // Make sure every generic is able to be mutated by `Record`
    // The redundant clone circumvents mutating the collection we're iterating over
    #[allow(clippy::redundant_clone)]
    for generic in input
        .generics
        .clone()
        .type_params()
        .map(|param| &param.ident)
    {
        input
            .generics
            .make_where_clause()
            .predicates
            .push(parse_quote! {
                differential_datalog::record::Record: differential_datalog::record::Mutator<#generic>
            });
    }

    // Add the required trait bounds
    let generics = add_trait_bounds(
        input.generics,
        vec![parse_quote!(differential_datalog::record::FromRecord)],
    );
    let generics = generics.split_for_impl();

    match input.data {
        // Derive for structs
        Data::Struct(derive_struct) => {
            mutator_struct(struct_ident, struct_record_name, derive_struct, generics)
        }

        // Derive for enums
        Data::Enum(derive_enum) => {
            mutator_enum(struct_ident, struct_record_name, derive_enum, generics)
        }

        // Unions can't safely/soundly be automatically implemented over,
        // the user will have to manually enforce invariants on it
        Data::Union(union) => Err(Error::new_spanned(
            union.union_token,
            "`Mutator` is not able to be automatically implemented on unions",
        )),
    }
}

/// Unit structs have nothing to mutate, so make sure the constructor is correct and that
/// there's no fields on the record
fn unit_struct_mutator(record_name: &str) -> TokenStream {
    quote! {
        match self {
            differential_datalog::record::Record::PosStruct(constructor, args)
                if &*constructor == #record_name && args.is_empty() => {},

            differential_datalog::record::Record::NamedStruct(constructor, args)
                if &*constructor == #record_name && args.is_empty() => {},

            error => {
                return std::result::Result::Err(std::format!("not a struct {:?}", error));
            },
        }
    }
}

fn tuple_struct_mutator<'a>(
    record_name: &str,
    tuple_struct: &'a FieldsUnnamed,
) -> (TokenStream, impl Iterator<Item = Ident> + 'a) {
    let num_fields = tuple_struct.unnamed.len();

    let indices = tuple_struct.unnamed.iter().enumerate().map(|(idx, field)| {
        let index = Index {
            index: idx as u32,
            span: field.span(),
        };

        format_ident!("_{}", index)
    });

    let field_mutations = tuple_struct
        .unnamed
        .iter()
        .zip(indices.clone())
        .enumerate()
        .map(|(idx, (field, index))| {
            let field_ty = &field.ty;

            quote! {
                <dyn differential_datalog::record::Mutator<#field_ty>>::mutate(&args[#idx], #index)?;
            }
        });

    let mutator = quote! {
        match self {
            differential_datalog::record::Record::PosStruct(constructor, args)
                if &*constructor == #record_name && args.len() == #num_fields => {
                    #( #field_mutations )*
                },

            error => {
                return std::result::Result::Err(std::format!("not a struct {:?}", error));
            },
        }
    };

    (mutator, indices)
}

fn named_struct_mutator<'a>(
    record_name: &str,
    named_struct: &'a FieldsNamed,
) -> Result<(TokenStream, impl Iterator<Item = &'a Ident> + 'a)> {
    let num_fields = named_struct.named.len();

    let field_mutations = named_struct.named.iter().map(|field| {
        let field_ty = &field.ty;
        let field_ident = field.ident.as_ref().expect("named structs have field names");

        let field_record_name = get_rename("Mutator", "into_record", field.attrs.iter())?
            .unwrap_or_else(|| field_ident.to_string());

        Ok(quote! {
            if let Some(field_record) = differential_datalog::record::arg_find(args, #field_record_name) {
                <dyn differential_datalog::record::Mutator<#field_ty>>::mutate(field_record, #field_ident)?;
            }
        })
    })
    .collect::<Result<TokenStream>>()?;

    let mutator = quote! {
        match self {
            differential_datalog::record::Record::NamedStruct(constructor, args)
                if &*constructor == #record_name && args.len() == #num_fields => {
                    #field_mutations
                },

            error => {
                return std::result::Result::Err(std::format!("not a struct {:?}", error));
            },
        }
    };

    let fields = named_struct.named.iter().map(|field| {
        field
            .ident
            .as_ref()
            .expect("named structs have field names")
    });

    Ok((mutator, fields))
}

fn mutator_struct(
    struct_ident: Ident,
    struct_record_name: String,
    derive_enum: DataStruct,
    (impl_generics, type_generics, where_clause): (
        ImplGenerics,
        TypeGenerics,
        Option<&WhereClause>,
    ),
) -> Result<TokenStream> {
    let generated_mutator = match derive_enum.fields {
        Fields::Named(named_struct) => {
            let (mutator, field_idents) = named_struct_mutator(&struct_record_name, &named_struct)?;

            quote! {
                let #struct_ident { #( #field_idents, )* } = mutated;
                #mutator
            }
        }
        Fields::Unnamed(unnamed_struct) => {
            let (mutator, tuple_elems) = tuple_struct_mutator(&struct_record_name, &unnamed_struct);

            quote! {
                let #struct_ident(#( #tuple_elems, )*) = mutated;
                #mutator
            }
        }
        Fields::Unit => unit_struct_mutator(&struct_record_name),
    };

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics differential_datalog::record::Mutator<#struct_ident #type_generics> for differential_datalog::record::Record #where_clause {
            fn mutate(&self, mutated: &mut #struct_ident #type_generics) -> std::result::Result<(), std::string::String> {
                #generated_mutator

                std::result::Result::Ok(())
            }
        }
    })
}

fn mutator_enum(
    enum_ident: Ident,
    enum_record_name: String,
    derive_enum: DataEnum,
    (impl_generics, type_generics, where_clause): (
        ImplGenerics,
        TypeGenerics,
        Option<&WhereClause>,
    ),
) -> Result<TokenStream> {
    let generated_mutators = derive_enum
        .variants
        .iter()
        .map(|variant| {
            let variant_ident = &variant.ident;
            let variant_record_name = get_rename("Mutator", "into_record", variant.attrs.iter())?
                .map_or_else(
                    || format!("{}::{}", enum_record_name, variant_ident),
                    |rename| format!("{}::{}", enum_record_name, rename),
                );

            let (guard, mutator) = match &variant.fields {
                Fields::Named(named_struct) => {
                    let (mutator, field_idents) =
                        named_struct_mutator(&variant_record_name, named_struct)?;
                    let guard = quote! { #enum_ident::#variant_ident { #( #field_idents, )* } };

                    (guard, mutator)
                }
                Fields::Unnamed(tuple_struct) => {
                    let (mutator, tuple_fields) =
                        tuple_struct_mutator(&variant_record_name, tuple_struct);
                    let guard = quote! { #enum_ident::#variant_ident(#( #tuple_fields, )*) };

                    (guard, mutator)
                }
                Fields::Unit => {
                    let guard = quote! { #enum_ident::#variant_ident };
                    let mutator = unit_struct_mutator(&variant_record_name);

                    (guard, mutator)
                }
            };

            Ok(quote! {
                #guard => #mutator,
            })
        })
        .collect::<Result<TokenStream>>()?;

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics differential_datalog::record::Mutator<#enum_ident #type_generics> for differential_datalog::record::Record #where_clause {
            fn mutate(&self, mutated: &mut #enum_ident #type_generics) -> std::result::Result<(), std::string::String> {
                match mutated {
                    #generated_mutators
                }

                std::result::Result::Ok(())
            }
        }
    })
}
