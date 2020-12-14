use types__tinyset::*;

impl<'a, T, F> FromFlatBuffer<fbrt::Vector<'a, F>> for Set64<T>
where
    T: Ord + FromFlatBuffer<F::Inner> + u64set::Fits64,
    F: fbrt::Follow<'a> + 'a,
{
    fn from_flatbuf(fb: fbrt::Vector<'a, F>) -> Result<Self, String> {
        let mut set = Set64::new();
        for x in FBIter::from_vector(fb) {
            set.insert(T::from_flatbuf(x)?);
        }
        Ok(set)
    }
}

// For scalar types, the FlatBuffers API returns slice instead of 'Vector'.
impl<'a, T> FromFlatBuffer<&'a [T]> for Set64<T>
where
    T: Ord + u64set::Fits64,
{
    fn from_flatbuf(fb: &'a [T]) -> Result<Self, String> {
        let mut set = Set64::new();
        for x in fb.iter() {
            set.insert(x.clone());
        }
        Ok(set)
    }
}

impl<'b, T> ToFlatBuffer<'b> for Set64<T>
where
    T: Ord + u64set::Fits64 + ToFlatBufferVectorElement<'b>,
{
    type Target = fbrt::WIPOffset<fbrt::Vector<'b, <T::Target as fbrt::Push>::Output>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec: Vec<T::Target> = self
            .iter()
            .map(|x| x.to_flatbuf_vector_element(fbb))
            .collect();
        fbb.create_vector(vec.as_slice())
    }
}
