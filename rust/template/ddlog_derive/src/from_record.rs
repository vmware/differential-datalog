use super::add_trait_bounds;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{
    parse_quote, spanned::Spanned, Attribute, Data, DataEnum, DataStruct, DeriveInput, Error,
    Ident, ImplGenerics, Lit, MetaNameValue, Result, TypeGenerics, WhereClause,
};

fn get_rename<'a, I>(macro_name: &str, specific_attr: &str, attrs: I) -> Result<Option<String>>
where
    I: Iterator<Item = &'a Attribute> + 'a,
{
    let mut renames = attrs
        .filter(|attr| attr.path.is_ident("ddlog"))
        .map(|attr| attr.parse_args::<MetaNameValue>())
        .map(|attr| {
            attr.and_then(|attr| {
                if attr.path.is_ident("rename") || attr.path.is_ident(specific_attr) {
                    Ok((attr.span(), attr.lit))
                } else {
                    Err(syn::Error::new_spanned(
                        attr.path,
                        format!(
                            "unrecognized attribute, expected `rename` or `{}`",
                            specific_attr,
                        ),
                    ))
                }
            })
        })
        .map(|lit| {
            lit.and_then(|(span, lit)| {
                if let Lit::Str(string) = lit {
                    Ok((span, string.value()))
                } else {
                    Err(Error::new_spanned(
                        lit,
                        format!(
                            "`{}` can only be renamed to string literal values",
                            macro_name,
                        ),
                    ))
                }
            })
        })
        .collect::<Result<Vec<(Span, String)>>>()?;

    if renames.is_empty() {
        Ok(None)
    } else if renames.len() == 1 {
        Ok(Some(renames.remove(0).1))
    } else {
        Err(Error::new(
            renames[0].0,
            format!(
                "got {} separate renames when only one is allowed",
                renames.len()
            ),
        ))
    }
}

pub fn from_record_inner(input: DeriveInput) -> Result<TokenStream> {
    // The name of the struct
    let struct_ident = input.ident;

    // Use the given rename provided by `#[ddlog(rename = "...")]` or `#[ddlog(from_record = "...")]`
    // as the name of the record, defaulting to the struct's ident if none is given
    let struct_record_name = get_rename("FromRecord", "from_record", input.attrs.iter())?
        .unwrap_or_else(|| struct_ident.to_string());

    // Add the required trait bounds
    let generics = add_trait_bounds(
        input.generics,
        vec![
            parse_quote!(differential_datalog::record::FromRecord),
            parse_quote!(Sized),
            parse_quote!(std::default::Default),
            parse_quote!(serde::de::DeserializeOwned),
        ],
    );
    let generics = generics.split_for_impl();

    match input.data {
        // Derive for structs
        Data::Struct(derive_struct) => {
            from_record_struct(struct_ident, struct_record_name, derive_struct, generics)
        }

        // Derive for enums
        Data::Enum(derive_enum) => {
            from_record_enum(struct_ident, struct_record_name, derive_enum, generics)
        }

        // Unions can't safely/soundly be automatically implemented over,
        // the user will have to manually enforce invariants on it
        Data::Union(union) => Err(Error::new_spanned(
            union.union_token,
            "`FromRecord` is not able to be automatically implemented on unions",
        )),
    }
}

fn from_record_struct(
    struct_name: Ident,
    struct_string: String,
    derive_struct: DataStruct,
    (impl_generics, type_generics, where_clause): (
        ImplGenerics,
        TypeGenerics,
        Option<&WhereClause>,
    ),
) -> Result<TokenStream> {
    // The total number of fields the struct has
    let num_fields = derive_struct.fields.len();

    // The innards of `FromRecord` for positional structs
    let positional_fields: TokenStream = derive_struct
        .fields.iter()
        .cloned()
        .enumerate()
        .map(|(idx, field)| {
            // Tuple structs have no field names, but instead use the tuple indexes
            let field_ident = field.ident.unwrap_or_else(|| parse_quote!(#idx));
            let field_type = field.ty;

            // Call `FromRecord::from_record()` directly on each field
            quote! {
                #field_ident: <#field_type as differential_datalog::record::FromRecord>::from_record(&args[#idx])?,
            }
        })
        .collect();

    // The innards of `FromRecord` for named structs
    let named_fields = derive_struct
        .fields
        .into_iter()
        .enumerate()
        .map(|(idx, field)| {
            // Tuple structs have no field names, but instead use the tuple indexes
            let field_ident = field.ident.unwrap_or_else(|| parse_quote!(#idx));
            let field_type = field.ty;

            // Use the given rename provided by `#[ddlog(rename = "...")]` or `#[ddlog(from_record = "...")]`
            // as the name of the field, defaulting to the field's ident if none is given
            let field_record_name = get_rename("FromRecord", "from_record", field.attrs.iter())?
                .unwrap_or_else(|| field_ident.to_string());

            // Call `FromRecord::from_record()` directly on each field
            Ok(quote! {
                #field_ident: differential_datalog::record::arg_extract::<#field_type>(args, #field_record_name)?,
            })
        })
        .collect::<Result<TokenStream>>()?;

    // Generate the actual code
    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics differential_datalog::record::FromRecord for #struct_name #type_generics #where_clause {
            fn from_record(record: &differential_datalog::record::Record) -> std::result::Result<Self, std::string::String> {
                match record {
                    differential_datalog::record::Record::PosStruct(constructor, args) => {
                        match constructor.as_ref() {
                            #struct_string if args.len() == #num_fields => {
                                std::result::Result::Ok(Self { #positional_fields })
                            },

                            error => {
                                std::result::Result::Err(format!(
                                    "unknown constructor {} of type '{}' in {:?}",
                                    error, #struct_string, *record,
                                ))
                            }
                        }
                    },

                    differential_datalog::record::Record::NamedStruct(constructor, args) => {
                        match constructor.as_ref() {
                            #struct_string => {
                                std::result::Result::Ok(Self { #named_fields })
                            },

                            error => {
                                std::result::Result::Err(format!(
                                    "unknown constructor {} of type '{}' in {:?}",
                                    error, #struct_string, *record,
                                ))
                            }
                        }
                    },

                    error => {
                        std::result::Result::Err(format!("not a struct {:?}", *error))
                    },
                }
            }
        }
    })
}

fn from_record_enum(
    enum_name: Ident,
    enum_string: String,
    derive_enum: DataEnum,
    (impl_generics, type_generics, where_clause): (
        ImplGenerics,
        TypeGenerics,
        Option<&WhereClause>,
    ),
) -> Result<TokenStream> {
    // Generate the code for extracting positional variants
    let positional_variants = derive_enum
        .variants
        .iter()
        .cloned()
        .map(|variant| {
            // The total number of fields on the variant
            let num_fields = variant.fields.len();
            let variant_ident = variant.ident;

            // Use the given rename provided by `#[ddlog(rename = "...")]` or `#[ddlog(from_record = "...")]`
            // as the name of the variant, defaulting to the variant's ident if none is given
            let variant_record_name = get_rename("FromRecord", "from_record", variant.attrs.iter())?
                .unwrap_or_else(|| variant_ident.to_string());

            let positional_fields: TokenStream = variant
                .fields
                .iter()
                .cloned()
                .enumerate()
                .map(|(idx, field)| {
                    // Tuple structs have no field names, but instead use the tuple indexes
                    let field_ident = field.ident.unwrap_or_else(|| parse_quote!(idx));
                    let field_type = field.ty;

                    // Call `FromRecord::from_record()` directly on each field
                    quote! {
                        #field_ident: <#field_type as differential_datalog::record::FromRecord>::from_record(&args[#idx])?,
                    }
                })
                .collect();

            // Generate the code for each match arm individually
            Ok(quote! {
                #variant_record_name if args.len() == #num_fields => {
                    std::result::Result::Ok(Self::#variant_ident { #positional_fields })
                },
            })
        })
        .collect::<Result<TokenStream>>()?;

    // Generate the code for extracting named variants
    let named_variants = derive_enum
        .variants
        .into_iter()
        .map(|variant| {
            // The total number of fields for the variant
            let num_fields = variant.fields.len();
            let variant_ident = variant.ident;

            // Use the given rename provided by `#[ddlog(rename = "...")]` or `#[ddlog(from_record = "...")]`
            // as the name of the variant, defaulting to the variant's ident if none is given
            let variant_record_name = get_rename("FromRecord", "from_record", variant.attrs.iter())?
                .unwrap_or_else(|| variant_ident.to_string());

            let named_fields = variant
                .fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    // Tuple structs have no field names, but instead use the tuple indexes
                    let field_ident = field.ident.unwrap_or_else(|| parse_quote!(#idx));
                    let field_type = field.ty;

                    // If the field is renamed within records then use that as the name to extract
                    let field_record_name = get_rename("FromRecord", "from_record", field.attrs.iter())?
                        .unwrap_or_else(|| field_ident.to_string());

                    // Call `FromRecord::from_record()` directly on each field
                    Ok(quote! {
                        #field_ident: differential_datalog::record::arg_extract::<#field_type>(args, #field_record_name)?,
                    })
                })
                .collect::<Result<TokenStream>>()?;

            // Generate the code for each match arm individually
            Ok(quote! {
                #variant_record_name if args.len() == #num_fields => {
                    std::result::Result::Ok(Self::#variant_ident { #named_fields })
                },
            })
        })
        .collect::<Result<TokenStream>>()?;

    Ok(quote! {
        #[automatically_derived]
        impl #impl_generics differential_datalog::record::FromRecord for #enum_name #type_generics #where_clause {
            fn from_record(record: &differential_datalog::record::Record) -> std::result::Result<Self, String> {
                match record {
                    differential_datalog::record::Record::PosStruct(constructor, args) => {
                        match constructor.as_ref() {
                            #positional_variants

                            error => {
                                std::result::Result::Err(format!(
                                    "unknown constructor {} of type '{}' in {:?}",
                                    error, #enum_string, *record,
                                ))
                            },
                        }
                    },

                    differential_datalog::record::Record::NamedStruct(constructor, args) => {
                        match constructor.as_ref() {
                            #named_variants

                            error => {
                                std::result::Result::Err(format!(
                                    "unknown constructor {} of type '{}' in {:?}",
                                    error, #enum_string, *record,
                                ))
                            }
                        }
                    },

                    error => {
                        std::result::Result::Err(format!("not a struct {:?}", *error))
                    }
                }
            }
        }
    })
}
