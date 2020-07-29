use url;

#[derive(Eq, Ord, Clone, Hash, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct url_Url {
    url: url::Url,
}

impl Default for url_Url {
    fn default() -> Self {
        url_Url {
            url: url::Url::parse("http://127.0.0.1/").unwrap(),
        }
    }
}

impl FromRecord for url_Url {
    fn from_record(val: &record::Record) -> result::Result<Self, String> {
        match (val) {
            record::Record::String(s) => match (url::Url::parse(&s)) {
                Ok(url) => Ok(url_Url { url }),
                Err(e) => Err(format!("{}", e)),
            },
            _ => Err(String::from("Unexpected type")),
        }
    }
}

impl IntoRecord for url_Url {
    fn into_record(self) -> record::Record {
        record::Record::String(self.url.as_str().to_string())
    }
}

impl record::Mutator<url_Url> for record::Record {
    fn mutate(&self, t: &mut url_Url) -> result::Result<(), String> {
        *t = url_Url::from_record(self)?;
        Ok(())
    }
}

pub fn url_url_parse(s: &String) -> std_Result<url_Url, String> {
    match url::Url::parse(s) {
        Ok(url) => std_Result::std_Ok {
            res: url_Url { url },
        },
        Err(e) => std_Result::std_Err {
            err: format!("{}", e),
        },
    }
}

pub fn url_join(url: &url_Url, other: &String) -> std_Result<url_Url, String> {
    match url.url.join(other.as_str()) {
        Ok(url) => std_Result::std_Ok {
            res: url_Url { url },
        },
        Err(e) => std_Result::std_Err {
            err: format!("{}", e),
        },
    }
}

pub fn url_url_to_string(url: &url_Url) -> String {
    format!("{}", url.url)
}
pub fn url_scheme(url: &url_Url) -> String {
    url.url.scheme().to_string()
}
pub fn url_has_authority(url: &url_Url) -> bool {
    url.url.has_authority()
}
pub fn url_cannot_be_a_base(url: &url_Url) -> bool {
    url.url.cannot_be_a_base()
}
pub fn url_username(url: &url_Url) -> String {
    url.url.username().to_string()
}
pub fn url_password(url: &url_Url) -> std_Option<String> {
    option2std(url.url.password().map(|x| x.to_string()))
}
pub fn url_has_host(url: &url_Url) -> bool {
    url.url.has_host()
}
pub fn url_host_str(url: &url_Url) -> std_Option<String> {
    option2std(url.url.host_str().map(|x| x.to_string()))
}
pub fn url_domain(url: &url_Url) -> std_Option<String> {
    option2std(url.url.domain().map(|x| x.to_string()))
}
pub fn url_port(url: &url_Url) -> std_Option<u16> {
    option2std(url.url.port())
}
pub fn url_port_or_known_default(url: &url_Url) -> std_Option<u16> {
    option2std(url.url.port_or_known_default())
}
pub fn url_path(url: &url_Url) -> String {
    url.url.path().to_string()
}
pub fn url_query(url: &url_Url) -> std_Option<String> {
    option2std(url.url.query().map(|x| x.to_string()))
}
pub fn url_fragment(url: &url_Url) -> std_Option<String> {
    option2std(url.url.fragment().map(|x| x.to_string()))
}
