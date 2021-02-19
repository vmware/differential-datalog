impl<'a, T, F> FromFlatBuffer<fbrt::Vector<'a, F>> for typedefs::hashset::HashSet<T>
where
    T: Hash + Eq + Clone + FromFlatBuffer<F::Inner>,
    F: fbrt::Follow<'a> + 'a,
{
    fn from_flatbuf(fb: fbrt::Vector<'a, F>) -> ::std::result::Result<Self, String> {
        let mut set = typedefs::hashset::HashSet::new();
        for x in FBIter::from_vector(fb) {
            set.insert(T::from_flatbuf(x)?);
        }
        Ok(set)
    }
}

// For scalar types, the FlatBuffers API returns slice instead of 'Vector'.
impl<'a, T> FromFlatBuffer<&'a [T]> for typedefs::hashset::HashSet<T>
where
    T: Hash + Eq + Clone,
{
    fn from_flatbuf(fb: &'a [T]) -> ::std::result::Result<Self, String> {
        let mut set = typedefs::hashset::HashSet::new();
        for x in fb.iter() {
            set.insert(x.clone());
        }
        Ok(set)
    }
}

impl<'b, T> ToFlatBuffer<'b> for typedefs::hashset::HashSet<T>
where
    T: Hash + Eq + Clone + Ord + ToFlatBufferVectorElement<'b>,
{
    type Target = fbrt::WIPOffset<fbrt::Vector<'b, <T::Target as fbrt::Push>::Output>>;

    fn to_flatbuf(&self, fbb: &mut fbrt::FlatBufferBuilder<'b>) -> Self::Target {
        let vec: ::std::vec::Vec<T::Target> = self
            .iter()
            .map(|x| x.to_flatbuf_vector_element(fbb))
            .collect();
        fbb.create_vector(vec.as_slice())
    }
}
