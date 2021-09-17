/*
Copyright (c) 2021 VMware, Inc.
SPDX-License-Identifier: MIT

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

impl<'a, T, F> FromFlatBuffer<fbrt::Vector<'a, F>> for crate::typedefs::hashset::HashSet<T>
where
    T: ::core::hash::Hash + ::core::cmp::Eq + ::core::clone::Clone + FromFlatBuffer<F::Inner>,
    F: fbrt::Follow<'a> + 'a,
    <F as fbrt::Follow<'a>>::Inner: Debug,
{
    fn from_flatbuf(
        fb: fbrt::Vector<'a, F>,
    ) -> ::core::result::Result<Self, ::std::string::String> {
        let mut set = crate::typedefs::hashset::HashSet::new();
        for x in FBIter::from_vector(fb) {
            set.insert(T::from_flatbuf(x)?);
        }

        Ok(set)
    }
}

// For scalar types, the FlatBuffers API returns slice instead of 'Vector'.
impl<'a, T> FromFlatBuffer<&'a [T]> for crate::typedefs::hashset::HashSet<T>
where
    T: ::core::hash::Hash + ::core::cmp::Eq + ::core::clone::Clone,
{
    fn from_flatbuf(fb: &'a [T]) -> ::core::result::Result<Self, ::std::string::String> {
        let mut set = crate::typedefs::hashset::HashSet::new();
        for x in fb.iter() {
            set.insert(x.clone());
        }

        Ok(set)
    }
}

impl<'b, T> ToFlatBuffer<'b> for crate::typedefs::hashset::HashSet<T>
where
    T: ::core::hash::Hash
        + ::core::cmp::Eq
        + ::core::clone::Clone
        + ::core::cmp::Ord
        + ToFlatBufferVectorElement<'b>,
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
