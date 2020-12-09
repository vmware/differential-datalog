impl<'a, Args, Output> FromFlatBuffer<&'a str> for Box<dyn ddlog_rt::Closure<Args, Output>>
where
    Args: 'static + Clone,
    Output: 'static + Clone,
{
    fn from_flatbuf(s: &'a str) -> Result<Self, String> {
        Err(format!("'from_flatbuf' not implemented for closures."))
    }
}

impl<'a, Args, Output> FromFlatBuffer<fb::__String<'a>> for Box<dyn ddlog_rt::Closure<Args, Output>>
where
    Args: 'static + Clone,
    Output: 'static + Clone,
{
    fn from_flatbuf(v: fb::__String<'a>) -> Result<Self, String> {
        Err(format!("'from_flatbuf' not implemented for closures."))
    }
}

impl<'b, Args, Output> ToFlatBuffer<'b> for Box<dyn ddlog_rt::Closure<Args, Output>>
where
    Args: 'static + Clone,
    Output: 'static + Clone,
{
    type Target = fbrt::WIPOffset<&'b str>;
    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        fbb.create_string(&format!("{}", self))
    }
}

impl<'b, Args, Output> ToFlatBufferTable<'b> for Box<dyn ddlog_rt::Closure<Args, Output>>
where
    Args: 'static + Clone,
    Output: 'static + Clone,
{
    type Target = fb::__String<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        let v = self.to_flatbuf(fbb);
        fb::__String::create(fbb, &fb::__StringArgs { v: Some(v) })
    }
}

impl<'b, Args, Output> ToFlatBufferVectorElement<'b> for Box<dyn ddlog_rt::Closure<Args, Output>>
where
    Args: 'static + Clone,
    Output: 'static + Clone,
{
    type Target = <Self as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}
