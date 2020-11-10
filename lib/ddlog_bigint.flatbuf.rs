impl<'a> FromFlatBuffer<fb::__BigInt<'a>> for ddlog_bigint::Int {
    fn from_flatbuf(fb: fb::__BigInt<'a>) -> std::result::Result<ddlog_bigint::Int, String> {
        let bytes = fb.bytes().ok_or_else(|| {
            format!("ddlog_bigint::Int::from_flatbuf: invalid buffer: failed to extract bytes")
        })?;
        Ok(ddlog_bigint::Int::from_bytes_be(fb.sign(), bytes))
    }
}

impl<'b> ToFlatBuffer<'b> for ddlog_bigint::Int {
    type Target = fbrt::WIPOffset<fb::__BigInt<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let (sign, bytes) = self.to_bytes_be();
        let vec = fbb.create_vector(&bytes);
        fb::__BigInt::create(
            fbb,
            &fb::__BigIntArgs {
                sign: sign != ::num::bigint::Sign::Minus,
                bytes: Some(vec),
            },
        )
    }
}

impl<'b> ToFlatBufferTable<'b> for ddlog_bigint::Int {
    type Target = fb::__BigInt<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.to_flatbuf(fbb)
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for ddlog_bigint::Int {
    type Target = <ddlog_bigint::Int as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}

impl<'a> FromFlatBuffer<fb::__BigUint<'a>> for ddlog_bigint::Uint {
    fn from_flatbuf(fb: fb::__BigUint<'a>) -> std::result::Result<ddlog_bigint::Uint, String> {
        let bytes = fb.bytes().ok_or_else(|| {
            format!("ddlog_bigint::Uint::from_flatbuf: invalid buffer: failed to extract bytes")
        })?;
        Ok(ddlog_bigint::Uint::from_bytes_be(bytes))
    }
}

impl<'b> ToFlatBuffer<'b> for ddlog_bigint::Uint {
    type Target = fbrt::WIPOffset<fb::__BigUint<'b>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec = fbb.create_vector(&self.to_bytes_be());
        fb::__BigUint::create(fbb, &fb::__BigUintArgs { bytes: Some(vec) })
    }
}

impl<'b> ToFlatBufferTable<'b> for ddlog_bigint::Uint {
    type Target = fb::__BigUint<'b>;
    fn to_flatbuf_table(
        &self,
        fbb: &mut fbrt::FlatBufferBuilder<'b>,
    ) -> fbrt::WIPOffset<Self::Target> {
        self.to_flatbuf(fbb)
    }
}

impl<'b> ToFlatBufferVectorElement<'b> for ddlog_bigint::Uint {
    type Target = <ddlog_bigint::Uint as ToFlatBuffer<'b>>::Target;

    fn to_flatbuf_vector_element(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        self.to_flatbuf(fbb)
    }
}
