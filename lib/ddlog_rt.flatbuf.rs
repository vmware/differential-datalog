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
