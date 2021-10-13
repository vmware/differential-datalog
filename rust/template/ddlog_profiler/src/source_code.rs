use serde::{ser::SerializeMap, Serialize, Serializer};
use std::{
    borrow::Cow,
    fmt,
    fmt::{Display, Formatter},
    str::Chars,
};

/// Location of a single character of a range of characters
/// in the source of the program.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SourcePosition {
    /// Contiguous code snippet:
    /// `sourcefile:line1.column1-line2.column2`.
    Range {
        /// Source file name.
        file: Cow<'static, str>,
        /// Line number of the first character of the snippet.
        start_line: u32,
        /// Column number of the first character of the snippet.
        start_col: u32,
        /// Line number of the last character of the snippet.
        end_line: u32,
        /// Column number of the last character of the snippet.
        end_col: u32,
    },
    /// Location of a single character:
    /// `sourcefile:line.column`.
    Location {
        /// Source file name.
        file: Cow<'static, str>,
        /// Line number.
        line: u32,
        /// Column number.
        col: u32,
    },
    /// Unknown location.
    Unknown,
}

impl SourcePosition {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn new_range(
        file: &'static str,
        start_line: u32,
        start_col: u32,
        end_line: u32,
        end_col: u32,
    ) -> SourcePosition {
        Self::Range {
            file: Cow::Borrowed(file),
            start_line,
            start_col,
            end_line,
            end_col,
        }
    }

    pub fn new_location(file: &'static str, line: u32, col: u32) -> SourcePosition {
        Self::Location {
            file: Cow::Borrowed(file),
            line,
            col,
        }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown)
    }
}

impl Default for SourcePosition {
    fn default() -> Self {
        Self::Unknown
    }
}

// Custom `Serialize` implementation that is more space-efficient
// than the auto-derived version.
impl Serialize for SourcePosition {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(None)?;
        match self {
            Self::Unknown => {}
            Self::Location { file, line, col } => {
                map.serialize_entry("file", file)?;
                map.serialize_entry("pos", &[line, col, line, col])?;
            }
            Self::Range {
                file,
                start_line,
                start_col,
                end_line,
                end_col,
            } => {
                map.serialize_entry("file", file)?;
                map.serialize_entry("pos", &[start_line, start_col, end_line, end_col])?;
            }
        }
        map.end()
    }
}

impl Display for SourcePosition {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), fmt::Error> {
        // We use the format prescribed by GNU coding standards:
        // http://www.gnu.org/prep/standards/standards.html#Errors
        match self {
            Self::Range {
                file,
                start_line,
                start_col,
                end_line,
                end_col,
            } => write!(
                f,
                "{}:{}.{}-{}.{}",
                file, start_line, start_col, end_line, end_col
            ),
            Self::Location { file, line, col } => write!(f, "{}:{}.{}", file, line, col),
            Self::Unknown => write!(f, "unknown location"),
        }
    }
}

/// A structure that stores the entire source code of the running program, used to generate
/// human-readable profiles with links to source code.
#[derive(Debug)]
pub struct DDlogSourceCode {
    /// Source code of all modules in the program.
    pub code: &'static [SourceFile],
}

/// Source code of a single DDlog module.
#[derive(Debug)]
pub struct SourceFile {
    /// Path to the file relative to the project root.  Used to reconstruct
    /// program sources in a single location as part of a profile.
    pub filename: &'static str,
    /// File contents (DDlog code).
    pub contents: &'static str,
}

/// Iterator over characters of a source file represented as an array of strings.
pub struct LinesIterator<'a> {
    // Text being iterated over.
    lines: &'a [&'a str],
    // Current line.
    line: u32,
    // Current column.
    col: u32,
    // Iterator over characters in the current line.
    chars: Option<Chars<'a>>,
    // Number of characters the iterator is allowed to output,
    // decremented each time the iterator yields a character.
    remaining_chars: u32,
    // Don't iterate beyond this location.
    end: Option<(u32, u32)>,
    // The last character returned by the iterator was a whitespace.
    last_whitespace: bool,
}

impl<'a> LinesIterator<'a> {
    pub fn new(
        lines: &'a [&'a str],
        mut start_line: u32,
        mut start_col: u32,
        end: Option<(u32, u32)>,
        len: u32,
    ) -> Self {
        if start_line == 0 {
            start_line = 1;
        };
        if start_col == 0 {
            start_col = 1;
        };
        Self {
            lines,
            line: start_line,
            col: start_col,
            chars: lines.get((start_line - 1) as usize).map(|l| {
                let mut chars = l.chars();
                for _ in 1..start_col {
                    chars.next();
                }
                chars
            }),
            remaining_chars: len,
            end,
            // This will cause `next()` to skip leading whitespace.
            last_whitespace: true,
        }
    }

    // Returns the next character in the snippet as is (without replacing newlines or
    // skipping whitespace chars).
    fn next_inner(&mut self) -> Option<char> {
        self.chars.as_ref()?;

        if let Some((last_line, last_col)) = self.end {
            if self.line > last_line || self.line == last_line && self.col >= last_col {
                return None;
            }
        }

        match self.chars.as_mut().unwrap().next() {
            // We're at the end of a line, start scanning next line.
            None => {
                self.line += 1;
                self.col = 1;
                self.chars = self.lines.get((self.line - 1) as usize).map(|l| l.chars());
                if self.chars.is_none() {
                    // Just finished scanning the last line.
                    None
                } else {
                    Some('\n')
                }
            }
            Some(c) => {
                self.col += 1;
                Some(c)
            }
        }
    }
}

impl<'a> Iterator for LinesIterator<'a> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_chars == 0 {
            return None;
        }

        if self.last_whitespace {
            // Skip whitespaces.
            let mut res = self.next_inner();
            loop {
                if res.is_none() || !res.unwrap().is_whitespace() {
                    break;
                }
                res = self.next_inner();
            }
            self.last_whitespace = false;
            match res {
                None => None,
                Some(c) => {
                    self.remaining_chars -= 1;
                    Some(c)
                }
            }
        } else {
            match self.next_inner() {
                None => None,
                Some(c) => {
                    self.remaining_chars -= 1;
                    if c.is_whitespace() {
                        self.last_whitespace = true;
                        Some(' ')
                    } else {
                        Some(c)
                    }
                }
            }
        }
    }
}
