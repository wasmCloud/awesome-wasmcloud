//! Configuration selected by both workload identity and import label.

use std::collections::BTreeMap;

use crate::config::Binding;

pub struct WorkloadBindings(BTreeMap<(String, Option<String>), Binding>);

impl WorkloadBindings {
    pub const fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn replace(&mut self, workload: String, bindings: Vec<(Option<String>, Binding)>) {
        self.remove(&workload);
        for (name, binding) in bindings {
            self.0.insert((workload.clone(), name), binding);
        }
    }

    pub fn get(&self, workload: String, name: Option<String>) -> Option<&Binding> {
        self.0.get(&(workload, name))
    }

    pub fn remove(&mut self, workload: &str) {
        self.0.retain(|(id, _), _| id != workload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(bucket: &str) -> Binding {
        Binding::from_config(
            &[
                ("endpoint", "https://cluster"),
                ("username", "app"),
                ("password", "secret"),
                ("bucket", bucket),
            ]
            .map(|(key, value)| (key.into(), value.into())),
        )
        .unwrap()
    }

    #[test]
    fn labels_and_workloads_select_distinct_keyspaces() {
        let mut bindings = WorkloadBindings::new();
        bindings.replace(
            "one".into(),
            vec![
                (None, binding("default")),
                (Some("orders".into()), binding("orders")),
                (Some("archive".into()), binding("archive")),
            ],
        );
        bindings.replace(
            "two".into(),
            vec![(Some("orders".into()), binding("other"))],
        );
        for (workload, name, bucket) in [
            ("one", None, "default"),
            ("one", Some("orders"), "orders"),
            ("one", Some("archive"), "archive"),
            ("two", Some("orders"), "other"),
        ] {
            assert_eq!(
                bindings
                    .get(workload.into(), name.map(str::to_owned))
                    .unwrap()
                    .bucket,
                bucket
            );
        }
        assert!(bindings.get("two".into(), None).is_none());
        assert!(bindings.get("one".into(), Some("missing".into())).is_none());

        bindings.replace(
            "one".into(),
            vec![(Some("orders".into()), binding("replacement"))],
        );
        assert!(bindings.get("one".into(), Some("archive".into())).is_none());
        assert!(bindings.get("one".into(), None).is_none());
        assert_eq!(
            bindings
                .get("one".into(), Some("orders".into()))
                .unwrap()
                .bucket,
            "replacement"
        );
        bindings.remove("one");
        assert!(bindings.get("one".into(), Some("orders".into())).is_none());
        assert_eq!(
            bindings
                .get("two".into(), Some("orders".into()))
                .unwrap()
                .bucket,
            "other"
        );
    }
}
