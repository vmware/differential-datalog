impl<'a, Args: 'static + Clone, Output: 'static + Clone> FromFlatBuffer<&'a str>
    for Box<dyn ddlog_rt::Closure<Args, Output>>
{
    fn from_flatbuf(s: &'a str) -> Result<Self, String> {
        Err(format!("'from_flatbuf' not implemented for closures."))
    }
}

impl<'a, Args: 'static + Clone, Output: 'static + Clone> FromFlatBuffer<fb::__String<'a>>
    for Box<dyn ddlog_rt::Closure<Args, Output>>
{
    fn from_flatbuf(v: fb::__String<'a>) -> Result<Self, String> {
        Err(format!("'from_flatbuf' not implemented for closures."))
    }
}

impl<'b, Args: 'static + Clone, Output: 'static + Clone> ToFlatBuffer<'b>
    for Box<dyn ddlog_rt::Closure<Args, Output>>
{
    type Target = fbrt::WIPOffset<&'b str>;
    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        fbb.create_string(&format!("{}", self))
    }
}

impl<'b, Args: 'static + Clone, Output: 'static + Clone> ToFlatBufferTable<'b>
    for Box<dyn ddlog_rt::Closure<Args, Output>>
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

impl<'b, Args: 'static + Clone, Output: 'static + Clone> ToFlatBufferVectorElement<'b>
    for Box<dyn ddlog_rt::Closure<Args, Output>>
{
    type Target = <Self as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}
