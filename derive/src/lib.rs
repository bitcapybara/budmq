use core::panic;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, FieldsNamed, Type};

#[proc_macro_derive(PacketCodec)]
pub fn packet_codec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let Data::Struct(struct_data) = input.data else {
        panic!("must derive on a struct")
    };
    let Fields::Named(fields) = struct_data.fields else {
        panic!("must derive on named fields")
    };

    let codec_struct = codec_struct(&struct_ident, &fields);
    quote! {
        #codec_struct

        impl #struct_ident {
            pub fn header(&self) -> crate::protocol::Header {
                use crate::protocol::Codec;
                crate::protocol::Header::new(crate::protocol::PacketType::#struct_ident, self.size())
            }
        }
    }
    .into()
}

#[proc_macro_derive(Codec)]
pub fn codec(input: TokenStream) -> TokenStream {
    let input: DeriveInput = parse_macro_input!(input);
    let input_ident = input.ident;

    match input.data {
        Data::Struct(struct_data) => {
            let Fields::Named(fields) = struct_data.fields else {
                panic!("must derive on named fields")
            };

            codec_struct(&input_ident, &fields).into()
        }
        Data::Enum(_) => codec_enum(&input_ident).into(),
        _ => panic!("unsupported input type"),
    }
}

fn codec_enum(input_ident: &proc_macro2::Ident) -> proc_macro2::TokenStream {
    quote! {
        impl crate::protocol::Codec for #input_ident {
            fn decode(buf: &mut bytes::Bytes) -> crate::protocol::Result<Self> {
                crate::protocol::get_u8(buf)?.try_into()
            }

            fn encode(&self, buf: &mut bytes::BytesMut) {
                use bytes::BufMut;
                buf.put_u8(*self as u8);
            }

            fn size(&self) -> usize {
                1
            }
        }
    }
}

fn codec_struct(
    input_ident: &proc_macro2::Ident,
    fields: &FieldsNamed,
) -> proc_macro2::TokenStream {
    let field_idents = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        quote! {
            #field_ident
        }
    });
    let field_decode_methods = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        let field_type_ident = get_field_type(field);
        match field_type_ident.to_string().as_str() {
            "u16" => quote! {
                let #field_ident = crate::protocol::get_u16(buf)?;
            },
            "u32" => quote! {
                let #field_ident = crate::protocol::get_u32(buf)?;
            },
            "u64" => quote! {
                let #field_ident = crate::protocol::get_u64(buf)?;
            },
            "String" => quote! {
                let #field_ident = crate::protocol::read_string(buf)?;
            },
            "Bytes" => quote! {
                let #field_ident = crate::protocol::read_bytes(buf)?;
            },
            _ => quote! {
                let #field_ident = #field_type_ident::decode(buf)?;
            },
        }
    });
    let field_encode_methods = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        match get_field_type(field).to_string().as_str() {
            "u16" => quote! {
                buf.put_u16(self.#field_ident);
            },
            "u32" => quote! {
                buf.put_u32(self.#field_ident);
            },
            "u64" => quote! {
                buf.put_u64(self.#field_ident);
            },
            "String" => quote! {
                crate::protocol::write_string(buf, &self.#field_ident);
            },
            "Bytes" => quote! {
                crate::protocol::write_bytes(buf, &self.#field_ident);
            },
            _ => quote! {
                self.#field_ident.encode(buf);
            },
        }
    });
    let field_sizes = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        match get_field_type(field).to_string().as_str() {
            "u16" => quote! {
                2
            },
            "u32" => quote! {
                4
            },
            "u64" => quote! {
                8
            },
            "String" | "Bytes" => quote! {
                2 + self.#field_ident.len()
            },
            _ => quote! {
                self.#field_ident.size()
            },
        }
    });
    quote! {
        impl crate::protocol::Codec for #input_ident {
            fn decode(buf: &mut bytes::Bytes) -> crate::protocol::Result<Self> {
                #(#field_decode_methods)*
                Ok(Self {
                    #(#field_idents),*
                })
            }

            fn encode(&self, buf: &mut bytes::BytesMut) {
                use bytes::BufMut;
                #(#field_encode_methods)*
            }

            fn size(&self) -> usize {
                #(#field_sizes) + *
            }
        }
    }
}

fn get_field_type(field: &Field) -> proc_macro2::Ident {
    let Type::Path(path) = &field.ty else {
        panic!("must derive on path field type")
    };
    path.path
        .segments
        .last()
        .expect("field type ident not found")
        .ident
        .clone()
}
