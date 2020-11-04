impl<'a> FromFlatBuffer<&'a str> for types__intern::IString {
    fn from_flatbuf(fb: &'a str) -> Result<Self, String> {
        Ok(types__intern::string_intern(&String::from_flatbuf(fb)?))
    }
}

impl<'b> ToFlatBuffer<'b> for types__intern::IString {
    type Target = fbrt::WIPOffset<&'b str>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        types__intern::istring_str(self).to_flatbuf(fbb)
    }
}

impl<'a> FromFlatBuffer<fb::__String<'a>> for types__intern::IString {
    fn from_flatbuf(v: fb::__String<'a>) -> Result<Self, String> {
        Ok(types__intern::string_intern(&String::from_flatbuf(v)?))
    }
}

impl<'b> ToFlatBufferTable<'b> for types__intern::IString {
    type Target = fb::__String<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        types__intern::istring_str(self).to_flatbuf_table(fbb)
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for types__intern::IString {
    type Target = fbrt::WIPOffset<&'b str>;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}
