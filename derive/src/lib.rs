use core::panic;

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Field, Fields, Type};

#[proc_macro_derive(Codec)]
pub fn codec(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_ident = input.ident;

    let Data::Struct(struct_data) = input.data else {
        panic!("must derive on a struct")
    };
    let Fields::Named(fields) = struct_data.fields else {
        panic!("must derive on named fields")
    };
    let field_idents = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        quote! {
            #field_ident
        }
    });
    let field_read_methods = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        match get_field_type(field).as_str() {
            "u64" => quote! {
                let #field_ident = super::get_u64(&mut buf)?;
            },
            "String" => quote! {
                let #field_ident = super::read_string(&mut buf)?;
            },
            "Bytes" => quote! {
                let #field_ident = super::read_bytes(&mut buf)?;
            },
            _ => panic!("unsupported field types"),
        }
    });
    let field_put_methods = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        match get_field_type(field).as_str() {
            "u64" => quote! {
                buf.put_u64(self.#field_ident);
            },
            "String" => quote! {
                super::write_string(buf, &self.#field_ident);
            },
            "Bytes" => quote! {
                super::write_bytes(buf, &self.#field_ident);
            },
            _ => panic!("unsupported field types"),
        }
    });
    let field_sizes = fields.named.iter().map(|field| {
        let field_ident = &field.ident;
        match get_field_type(field).as_str() {
            "u64" => quote! {
                8
            },
            "String" | "Bytes" => quote! {
                2 + self.#field_ident.len()
            },
            "u16" => quote! {
                2
            },
            _ => panic!("unsupported field types"),
        }
    });
    quote! {
        use bytes::{BufMut, Bytes};
        impl super::Codec for #struct_ident {
            fn decode(mut buf: bytes::Bytes) -> super::Result<Self> {
                #(#field_read_methods)*
                Ok(Self {
                    #(#field_idents),*
                })
            }

            fn encode(&self, buf: &mut bytes::BytesMut) -> super::Result<()> {
                #(#field_put_methods)*
                Ok(())
            }

            fn header(&self) -> super::Header {
                super::Header::new(super::PacketType::#struct_ident, #(#field_sizes) + *)
            }
        }
    }
    .into()
}

fn get_field_type(field: &Field) -> String {
    let Type::Path(path) = &field.ty else {
        panic!("must derive on path field type")
    };
    path.path
        .segments
        .last()
        .expect("field type ident not found")
        .ident
        .to_string()
}
