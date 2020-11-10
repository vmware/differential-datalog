impl<A, FB> FromFlatBuffer<FB> for internment::Intern<A>
where
    A: Eq + std::hash::Hash + Send + Sync + 'static,
    A: FromFlatBuffer<FB>,
{
    fn from_flatbuf(fb: FB) -> Result<Self, String> {
        Ok(internment::Intern::new(A::from_flatbuf(fb)?))
    }
}

impl<'b, A, T> ToFlatBuffer<'b> for internment::Intern<A>
where
    T: 'b,
    A: Eq + Send + Sync + std::hash::Hash + ToFlatBuffer<'b, Target = T>,
{
    type Target = T;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.as_ref().to_flatbuf(fbb)
    }
}

/*#[cfg(feature = "flatbuf")]
impl<'a> FromFlatBuffer<fb::__String<'a>> for intern_istring {
    fn from_flatbuf(v: fb::__String<'a>) -> Response<Self> {
        Ok(intern_string_intern(&String::from_flatbuf(v)?))
    }
}*/

impl<'b, A, T> ToFlatBufferTable<'b> for internment::Intern<A>
where
    T: 'b,
    A: Eq + Send + Sync + std::hash::Hash + ToFlatBufferTable<'b, Target = T>,
{
    type Target = T;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.as_ref().to_flatbuf_table(fbb)
    }
}

impl<'b, A, T> ToFlatBufferVectorElement<'b> for internment::Intern<A>
where
    T: 'b + fbrt::Push + Copy,
    A: Eq + Send + Sync + std::hash::Hash + ToFlatBufferVectorElement<'b, Target = T>,
{
    type Target = T;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.as_ref().to_flatbuf_vector_element(fbb)
    }
}
