use proc_macro2::TokenStream;
use quote::quote;
use syn::{
    parse_macro_input, parse_quote, Data, DeriveInput, Error, GenericParam, Generics, Lit,
    MetaNameValue, Result, TypeParamBound,
};

#[proc_macro_derive(FromRecord)]
pub fn derive_from_record(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    from_record_inner(input)
        .unwrap_or_else(|err| err.to_compile_error())
        .into()
}

fn from_record_inner(input: DeriveInput) -> Result<TokenStream> {
    // If `#[ddlog(rename = "...")]` or `#[ddlog(from_record = "")] is provided, the struct will be renamed to the given string
    let struct_name = input
        .attrs
        .iter()
        .filter(|attr| attr.path.is_ident("ddlog"))
        .map(|attr| attr.parse_args::<MetaNameValue>())
        .find(|attr| {
            attr.as_ref()
                .map(|attr| attr.path.is_ident("rename") || attr.path.is_ident("from_record"))
                .unwrap_or_default()
        })
        .transpose()?
        .map(|meta| {
            if let Lit::Str(string) = meta.lit {
                Ok(string.value())
            } else {
                Err(Error::new_spanned(
                    meta.lit,
                    "`FromRecord` can only be renamed to string literal values",
                ))
            }
        })
        .transpose()?
        .unwrap_or_else(|| input.ident.to_string());

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
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    match input.data {
        Data::Struct(derive_struct) => {
            let num_fields = derive_struct.fields.len();

            // The innards of `FromRecord` for positional structs
            let positional_fields: TokenStream = derive_struct.fields.iter().cloned().enumerate().map(|(idx, field)| {
                // Tuple structs have no field names, but instead use the tuple indexes
                let field_name = field.ident.unwrap_or_else(|| parse_quote!(idx));
                let field_type = field.ty;

                quote! {
                    #field_name: <#field_type as differential_datalog::record::FromRecord>::from_record(&args[#idx])?,
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
                    let field_name = field.ident.unwrap_or_else(|| parse_quote!(#idx));
                    let field_type = field.ty;

                    // If the field is renamed within records then use that as the name to extract
                    let record_name = field
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("ddlog"))
                        .map(|attr| attr.parse_args::<MetaNameValue>())
                        .find(|attr| {
                            attr.as_ref()
                                .map(|attr| attr.path.is_ident("rename") || attr.path.is_ident("from_record"))
                                .unwrap_or_default()
                        })
                        .transpose()?
                        .map(|meta| {
                            if let Lit::Str(string) = meta.lit {
                                Ok(string.value())
                            } else {
                                Err(Error::new_spanned(
                                    meta.lit,
                                    "`FromRecord` fields can only be renamed to string literal values",
                                ))
                            }
                        })
                        .transpose()?
                        .unwrap_or_else(|| field_name.to_string());

                    Ok(quote! {
                        #field_name: differential_datalog::record::arg_extract::<#field_type>(args, #record_name)?,
                    })
                })
                .collect::<Result<TokenStream>>()?;

            Ok(quote! {
                #[automatically_derived]
                impl #impl_generics differential_datalog::record::FromRecord for #struct_name #type_generics #where_clause {
                    fn from_record(record: &differential_datalog::record::Record) -> std::result::Result<Self, std::string::String> {
                        match record {
                            differential_datalog::record::Record::PosStruct(constructor, args) => {
                                match constructor.as_ref() {
                                    #struct_name if args.len() == #num_fields => {
                                        std::result::Result::Ok(Self { #positional_fields })
                                    },

                                    error => {
                                        std::result::Result::Err(format!(
                                            "unknown constructor {} of type '{}' in {:?}",
                                            error, #struct_name, *record,
                                        ))
                                    }
                                }
                            },

                            differential_datalog::record::Record::NamedStruct(constructor, args) => {
                                match constructor.as_ref() {
                                    #struct_name => {
                                        std::result::Result::Ok(Self { #named_fields })
                                    },

                                    error => {
                                        std::result::Result::Err(format!(
                                            "unknown constructor {} of type '{}' in {:?}",
                                            error, #struct_name, *record,
                                        ))
                                    }
                                }
                            },

                            differential_datalog::record::Record::Serialized(format, serialized) => {
                                if format == "json" {
                                    serde_json::from_str(&*serialized).map_err(|error| format!("{}", error))
                                } else {
                                    std::result::Result::Err(format!("unsupported serialization format '{}'", format))
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

        Data::Enum(derive_enum) => {
            let positional_variants = derive_enum
                .variants
                .iter()
                .cloned()
                .map(|variant| {
                    let num_fields = variant.fields.len();
                    let variant_name = variant.ident;

                    // If the variant is renamed within records then use that as the name to extract
                    let record_name = variant
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("ddlog"))
                        .map(|attr| attr.parse_args::<MetaNameValue>())
                        .find(|attr| {
                            attr.as_ref()
                                .map(|attr| attr.path.is_ident("rename") || attr.path.is_ident("from_record"))
                                .unwrap_or_default()
                        })
                        .transpose()?
                        .map(|meta| {
                            if let Lit::Str(string) = meta.lit {
                                Ok(string.value())
                            } else {
                                Err(Error::new_spanned(
                                    meta.lit,
                                    "`FromRecord` variants can only be renamed to string literal values",
                                ))
                            }
                        })
                        .transpose()?
                        .unwrap_or_else(|| variant_name.to_string());

                    let positional_fields: TokenStream = variant
                        .fields
                        .iter()
                        .cloned()
                        .enumerate()
                        .map(|(idx, field)| {
                            // Tuple structs have no field names, but instead use the tuple indexes
                            let field_name = field.ident.unwrap_or_else(|| parse_quote!(idx));
                            let field_type = field.ty;

                            quote! {
                                #field_name: <#field_type as differential_datalog::record::FromRecord>::from_record(&args[#idx])?,
                            }
                        })
                        .collect();

                    Ok(quote! {
                        #record_name if args.len() == #num_fields => {
                            std::result::Result::Ok(Self::#variant_name { #positional_fields })
                        },
                    })
                })
                .collect::<Result<TokenStream>>()?;

            let named_variants = derive_enum
                .variants
                .into_iter()
                .map(|variant| {
                    let num_fields = variant.fields.len();
                    let variant_name = variant.ident;

                    // If the variant is renamed within records then use that as the name to extract
                    let record_name = variant
                        .attrs
                        .iter()
                        .filter(|attr| attr.path.is_ident("ddlog"))
                        .map(|attr| attr.parse_args::<MetaNameValue>())
                        .find(|attr| {
                            attr.as_ref()
                                .map(|attr| attr.path.is_ident("rename") || attr.path.is_ident("from_record"))
                                .unwrap_or_default()
                        })
                        .transpose()?
                        .map(|meta| {
                            if let Lit::Str(string) = meta.lit {
                                Ok(string.value())
                            } else {
                                Err(Error::new_spanned(
                                    meta.lit,
                                    "`FromRecord` variants can only be renamed to string literal values",
                                ))
                            }
                        })
                        .transpose()?
                        .unwrap_or_else(|| variant_name.to_string());

                        let named_fields = variant
                            .fields
                            .into_iter()
                            .enumerate()
                            .map(|(idx, field)| {
                                // Tuple structs have no field names, but instead use the tuple indexes
                                let field_name = field.ident.unwrap_or_else(|| parse_quote!(#idx));
                                let field_type = field.ty;

                                // If the field is renamed within records then use that as the name to extract
                                let record_name = field
                                    .attrs
                                    .iter()
                                    .filter(|attr| attr.path.is_ident("ddlog"))
                                    .map(|attr| attr.parse_args::<MetaNameValue>())
                                    .find(|attr| {
                                        attr.as_ref()
                                            .map(|attr| attr.path.is_ident("rename") || attr.path.is_ident("from_record"))
                                            .unwrap_or_default()
                                    })
                                    .transpose()?
                                    .map(|meta| {
                                        if let Lit::Str(string) = meta.lit {
                                            Ok(string.value())
                                        } else {
                                            Err(Error::new_spanned(
                                                meta.lit,
                                                "`FromRecord` fields can only be renamed to string literal values",
                                            ))
                                        }
                                    })
                                    .transpose()?
                                    .unwrap_or_else(|| field_name.to_string());

                                Ok(quote! {
                                    #field_name: differential_datalog::record::arg_extract::<#field_type>(args, #record_name)?,
                                })
                            })
                            .collect::<Result<TokenStream>>()?;

                    Ok(quote! {
                        #record_name if args.len() == #num_fields => {
                            std::result::Result::Ok(Self::#variant_name { #named_fields })
                        },
                    })
                })
                .collect::<Result<TokenStream>>()?;

            Ok(quote! {
                #[automatically_derived]
                impl #impl_generics differential_datalog::record::FromRecord for #struct_name #type_generics #where_clause {
                    fn from_record(record: &differential_datalog::record::Record) -> std::result::Result<Self, String> {
                        match record {
                            differential_datalog::record::Record::PosStruct(constructor, args) => {
                                match constructor.as_ref() {
                                    #positional_variants

                                    error => {
                                        std::result::Result::Err(format!(
                                            "unknown constructor {} of type '{}' in {:?}",
                                            error, #struct_name, *record,
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
                                            error, #struct_name, *record,
                                        ))
                                    }
                                }
                            },

                            differential_datalog::record::Record::Serialized(format, serialized) => {
                                if format == "json" {
                                    serde_json::from_str(&*serialized).map_err(|error| format!("{}", error))
                                } else {
                                    std::result::Result::Err(format!("unsupported serialization format '{}'", format))
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

        Data::Union(union) => Err(Error::new_spanned(
            union.union_token,
            "`FromRecord` is not able to be automatically implemented on unions",
        )),
    }
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
