//! Bundle metadata, observed through the crate that defines TOML's grammar
//! (M2-K19 round-3 ruling, M2-K16). `bundle.contains("[recovery]")` passed
//! on a COMMENT while the real header was the malformed `[equality].`;
//! `DeTable::parse` refuses that file, and [`Metadata::string_at`] walks
//! real tables, so a header that merely spells the text satisfies nothing.

use toml::de::{DeTable, DeValue};

/// One parsed bundle. The document is re-parsed per query — cheap, and it
/// keeps the borrowed `DeTable` out of the type.
pub struct Metadata {
    path: String,
    text: String,
}

impl Metadata {
    /// Parse a bundle; a malformed document panics here naming the file.
    pub fn parse(path: &str, text: &str) -> Metadata {
        if let Err(refused) = DeTable::parse(text) {
            panic!("{path} is well-formed TOML: {refused}");
        }
        Metadata {
            path: path.to_owned(),
            text: text.to_owned(),
        }
    }

    fn root(&self) -> toml::Spanned<DeTable<'_>> {
        DeTable::parse(&self.text)
            .unwrap_or_else(|refused| panic!("{} is well-formed TOML: {refused}", self.path))
    }

    fn walk<T>(&self, path: &str, leaf: impl FnOnce(&DeValue<'_>) -> Option<T>) -> Option<T> {
        let root = self.root();
        let mut segments = path.split('.').peekable();
        let mut table = root.get_ref();
        while let Some(segment) = segments.next() {
            let value = entry(table, segment)?;
            if segments.peek().is_none() {
                return leaf(value);
            }
            table = value.as_table()?;
        }
        None
    }

    /// The string a dotted path names, or `None` when the path is absent
    /// or does not end at a string.
    pub fn string_at(&self, path: &str) -> Option<String> {
        self.walk(path, |value| value.as_str().map(str::to_owned))
    }

    /// The integer a dotted path names.
    pub fn integer_at(&self, path: &str) -> Option<i64> {
        self.walk(path, |value| {
            value
                .as_integer()
                .and_then(|integer| i64::from_str_radix(integer.as_str(), integer.radix()).ok())
        })
    }

    /// Whether a dotted path names a TABLE (`[a.b]`), as parsed.
    pub fn has_table(&self, path: &str) -> bool {
        self.walk(path, |value| value.as_table().map(|_| ()))
            .is_some()
    }

    /// Whether a dotted path names any key at all.
    pub fn has_key(&self, path: &str) -> bool {
        self.walk(path, |_| Some(())).is_some()
    }

    /// The keys of the table a dotted path names, in document order.
    pub fn keys(&self, path: &str) -> Vec<String> {
        self.walk(path, |value| {
            value
                .as_table()
                .map(|table| table.keys().map(|key| key.get_ref().to_string()).collect())
        })
        .unwrap_or_default()
    }

    /// `[contract].name`, the bundle's own statement of which contract it
    /// describes.
    pub fn name(&self) -> String {
        self.string_at("contract.name")
            .unwrap_or_else(|| panic!("{} declares [contract].name", self.path))
    }

    /// `[contract].version`.
    pub fn version(&self) -> String {
        self.string_at("contract.version")
            .unwrap_or_else(|| panic!("{} declares [contract].version", self.path))
    }
}

/// The value under `key` in `table`, whatever its type. `DeTable` keys are
/// spanned, so they are compared through their text.
fn entry<'a, 'i>(table: &'a DeTable<'i>, key: &str) -> Option<&'a DeValue<'i>> {
    table
        .iter()
        .find(|(name, _)| name.get_ref().as_ref() == key)
        .map(|(_, value)| value.get_ref())
}
