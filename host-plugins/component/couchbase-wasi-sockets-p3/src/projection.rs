//! Local projection of simple object paths. More elaborate Couchbase path
//! syntax is refused explicitly rather than misreported as a missing field.

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    NotJson,
    InvalidPath,
    TooDeep,
    UnsupportedPath,
}

pub fn project(document: &[u8], paths: &[String]) -> Result<Vec<u8>, Error> {
    for path in paths {
        if path.contains(['[', ']', '`', '\\']) {
            return Err(Error::UnsupportedPath);
        }
        if path.is_empty() || path.len() > 1024 || path.split('.').any(str::is_empty) {
            return Err(Error::InvalidPath);
        }
        if path.split('.').count() > 32 {
            return Err(Error::TooDeep);
        }
    }
    let parsed: serde_json::Value = serde_json::from_slice(document).map_err(|_| Error::NotJson)?;
    let mut out = serde_json::Map::new();
    for path in paths {
        let mut node = Some(&parsed);
        for segment in path.split('.') {
            node = node.and_then(|n| n.get(segment));
        }
        let Some(value) = node else { continue };
        let mut cursor = &mut out;
        let mut keys = path.split('.').peekable();
        while let Some(key) = keys.next() {
            if keys.peek().is_none() {
                cursor.insert(key.to_string(), value.clone());
                break;
            }
            let entry = cursor
                .entry(key.to_string())
                .or_insert_with(|| serde_json::Value::Object(serde_json::Map::new()));
            cursor = entry
                .as_object_mut()
                .expect("an existing parent comes from the same document");
        }
    }
    Ok(serde_json::Value::Object(out).to_string().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_and_escaped_paths_are_not_silently_omitted() {
        let document = br#"{"items":[{"price":10}],"a.b":4}"#;
        for path in ["items[0].price", "items[-1]", "`a.b`", "`a..b`", "a\\.b"] {
            assert_eq!(
                project(document, &[path.into()]),
                Err(Error::UnsupportedPath)
            );
        }
    }

    #[test]
    fn nested_object_paths_preserve_shape_and_absent_fields_stay_absent() {
        let document = br#"{"name":"Ada","address":{"city":"London","zip":1}}"#;
        let projected = project(document, &["address.city".into(), "missing".into()]).unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&projected).unwrap(),
            serde_json::json!({"address":{"city":"London"}})
        );
        for paths in [
            vec!["address", "address.city"],
            vec!["address.city", "address"],
        ] {
            let projected = project(
                document,
                &paths.into_iter().map(str::to_owned).collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&projected).unwrap(),
                serde_json::json!({"address":{"city":"London","zip":1}})
            );
        }
    }

    #[test]
    fn malformed_paths_and_non_json_documents_fail() {
        for path in ["", ".a", "a.", "a..b"] {
            assert_eq!(project(b"{}", &[path.into()]), Err(Error::InvalidPath));
        }
        assert_eq!(
            project(b"{}", &[vec!["a"; 33].join(".")]),
            Err(Error::TooDeep)
        );
        assert_eq!(project(b"not json", &["a".into()]), Err(Error::NotJson));
    }
}
