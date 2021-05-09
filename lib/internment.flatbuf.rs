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
