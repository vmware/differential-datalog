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

use differential_datalog::record;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct Url {
    url: ::url::Url,
}

impl Default for Url {
    fn default() -> Self {
        Url {
            url: ::url::Url::parse("http://127.0.0.1/").unwrap(),
        }
    }
}

impl FromRecordInner for Url {
    fn from_record_inner(val: &record::Record) -> ::std::result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (::url::Url::parse(&s)) {
                Ok(url) => Ok(Url { url }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for Url {
    fn into_record(self) -> record::Record {
        record::Record::String(self.url.as_str().to_string())
    }
}

impl record::Mutator<Url> for record::Record {
    fn mutate(&self, t: &mut Url) -> ::std::result::Result<(), String> {
        *t = Url::from_record(self)?;
        Ok(())
    }
}

pub fn url_parse(s: &String) -> ddlog_std::Result<Url, String> {
    match ::url::Url::parse(s) {
        Ok(url) => ddlog_std::Result::Ok { res: Url { url } },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn join(url: &Url, other: &String) -> ddlog_std::Result<Url, String> {
    match url.url.join(other.as_str()) {
        Ok(url) => ddlog_std::Result::Ok { res: Url { url } },
        Err(e) => ddlog_std::Result::Err {
            err: format!("{}", e),
        },
    }
}

pub fn url_to_string(url: &Url) -> String {
    format!("{}", url.url)
}
pub fn scheme(url: &Url) -> String {
    url.url.scheme().to_string()
}
pub fn has_authority(url: &Url) -> bool {
    url.url.has_authority()
}
pub fn cannot_be_a_base(url: &Url) -> bool {
    url.url.cannot_be_a_base()
}
pub fn username(url: &Url) -> String {
    url.url.username().to_string()
}
pub fn password(url: &Url) -> ddlog_std::Option<String> {
    url.url.password().map(|x| x.to_string()).into()
}
pub fn has_host(url: &Url) -> bool {
    url.url.has_host()
}
pub fn host_str(url: &Url) -> ddlog_std::Option<String> {
    url.url.host_str().map(|x| x.to_string()).into()
}
pub fn domain(url: &Url) -> ddlog_std::Option<String> {
    url.url.domain().map(|x| x.to_string()).into()
}
pub fn port(url: &Url) -> ddlog_std::Option<u16> {
    url.url.port().into()
}
pub fn port_or_known_default(url: &Url) -> ddlog_std::Option<u16> {
    url.url.port_or_known_default().into()
}
pub fn path(url: &Url) -> String {
    url.url.path().to_string()
}
pub fn query(url: &Url) -> ddlog_std::Option<String> {
    url.url.query().map(|x| x.to_string()).into()
}
pub fn fragment(url: &Url) -> ddlog_std::Option<String> {
    url.url.fragment().map(|x| x.to_string()).into()
}
