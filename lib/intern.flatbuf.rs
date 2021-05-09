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
