//! Workload ownership of idle connections, independent of the socket bindings.

use std::collections::{BTreeMap, BTreeSet};

use crate::config::{Binding, ConnectionKey};

struct Workload {
    generation: u64,
    bindings: BTreeMap<Option<String>, Binding>,
}

pub struct BindToken {
    workload: String,
    generation: u64,
}

pub struct State<C> {
    generation: u64,
    workloads: BTreeMap<String, Workload>,
    connections: BTreeMap<ConnectionKey, C>,
}

impl<C> State<C> {
    pub const fn new() -> Self {
        Self {
            generation: 0,
            workloads: BTreeMap::new(),
            connections: BTreeMap::new(),
        }
    }

    pub fn start_bind(&mut self, workload: String) -> BindToken {
        self.generation = self
            .generation
            .checked_add(1)
            .expect("binding generation exhausted");
        let entry = self
            .workloads
            .entry(workload.clone())
            .or_insert_with(|| Workload {
                generation: self.generation,
                bindings: BTreeMap::new(),
            });
        entry.generation = self.generation;
        BindToken {
            workload,
            generation: self.generation,
        }
    }

    fn is_current(&self, token: &BindToken) -> bool {
        self.workloads
            .get(&token.workload)
            .is_some_and(|w| w.generation == token.generation)
    }

    /// Connections opened while validating a bind stay local until every entry
    /// succeeds. A timed-out bind cannot publish after its unbind or replacement.
    pub fn commit_bind(
        &mut self,
        token: BindToken,
        bindings: Vec<(Option<String>, Binding)>,
        opened: BTreeMap<ConnectionKey, C>,
    ) -> bool {
        if !self.is_current(&token) {
            return false;
        }
        self.workloads
            .get_mut(&token.workload)
            .expect("current bind")
            .bindings = bindings.into_iter().collect();
        self.prune();
        for (key, connection) in opened {
            self.return_connection(key, connection);
        }
        true
    }

    pub fn cancel_bind(&mut self, token: BindToken) {
        if self.is_current(&token) {
            self.unbind(&token.workload);
        }
    }

    pub fn unbind(&mut self, workload: &str) {
        self.workloads.remove(workload);
        self.prune();
    }

    pub fn binding(&self, workload: &str, name: Option<String>) -> Option<&Binding> {
        self.workloads.get(workload)?.bindings.get(&name)
    }

    pub fn has_connection(&self, key: &ConnectionKey) -> bool {
        self.connections.contains_key(key)
    }

    pub fn take_connection(&mut self, key: &ConnectionKey) -> Option<C> {
        self.connections.remove(key)
    }

    fn owns(&self, key: &ConnectionKey) -> bool {
        self.workloads
            .values()
            .any(|w| w.bindings.values().any(|b| b.connection_key() == *key))
    }

    /// An in-flight call may finish after the last owner unbinds. Its socket
    /// must be dropped here rather than resurrecting an orphaned cache entry.
    pub fn return_connection(&mut self, key: ConnectionKey, connection: C) {
        if self.owns(&key) {
            self.connections.insert(key, connection);
        }
    }

    fn prune(&mut self) {
        let owned: BTreeSet<_> = self
            .workloads
            .values()
            .flat_map(|w| w.bindings.values().map(Binding::connection_key))
            .collect();
        self.connections.retain(|key, _| owned.contains(key));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    struct Socket(Rc<Cell<usize>>);
    impl Socket {
        fn new(live: &Rc<Cell<usize>>) -> Self {
            live.set(live.get() + 1);
            Self(live.clone())
        }
    }
    impl Drop for Socket {
        fn drop(&mut self) {
            self.0.set(self.0.get() - 1);
        }
    }
    fn binding(password: &str) -> Binding {
        Binding::from_config(
            &[
                ("endpoint", "couchbase://backend"),
                ("bucket", "test"),
                ("username", "app"),
                ("password", password),
            ]
            .map(|(k, v)| (k.into(), v.into())),
        )
        .unwrap()
    }
    fn bind(state: &mut State<Socket>, workload: &str, binding: Binding) {
        let token = state.start_bind(workload.into());
        assert!(state.commit_bind(token, vec![(None, binding)], BTreeMap::new()));
    }

    #[test]
    fn shared_connection_closes_only_after_the_final_owner_unbinds() {
        let live = Rc::new(Cell::new(0));
        let mut state = State::new();
        let b = binding("shared");
        bind(&mut state, "one", b.clone());
        bind(&mut state, "two", b.clone());
        state.return_connection(b.connection_key(), Socket::new(&live));
        state.unbind("one");
        assert_eq!(live.get(), 1);
        assert!(state.binding("one", None).is_none());
        assert!(state.has_connection(&b.connection_key()));
        state.unbind("two");
        assert_eq!(live.get(), 0);
        assert!(state.connections.is_empty());
    }

    #[test]
    fn an_inflight_call_cannot_recache_an_orphan_after_unbind() {
        let live = Rc::new(Cell::new(0));
        let mut state = State::new();
        let b = binding("shared");
        bind(&mut state, "one", b.clone());
        state.return_connection(b.connection_key(), Socket::new(&live));
        let socket = state.take_connection(&b.connection_key()).unwrap();
        state.unbind("one");
        assert_eq!(live.get(), 1);
        state.return_connection(b.connection_key(), socket);
        assert_eq!(live.get(), 0);
        assert!(state.connections.is_empty());
    }

    #[test]
    fn an_inflight_shared_connection_can_return_to_a_remaining_owner() {
        let live = Rc::new(Cell::new(0));
        let mut state = State::new();
        let b = binding("shared");
        bind(&mut state, "one", b.clone());
        bind(&mut state, "two", b.clone());
        state.return_connection(b.connection_key(), Socket::new(&live));
        let socket = state.take_connection(&b.connection_key()).unwrap();
        state.unbind("one");
        state.return_connection(b.connection_key(), socket);
        assert_eq!(live.get(), 1);
        assert!(state.has_connection(&b.connection_key()));
    }

    #[test]
    fn cancelled_bind_cannot_publish_connections_or_replace_a_newer_bind() {
        let live = Rc::new(Cell::new(0));
        let mut state = State::new();
        let old = binding("old");
        let token = state.start_bind("one".into());
        let opened = BTreeMap::from([(old.connection_key(), Socket::new(&live))]);
        state.unbind("one");
        let new = binding("new");
        bind(&mut state, "one", new.clone());
        assert!(!state.commit_bind(token, vec![(None, old)], opened));
        assert_eq!(live.get(), 0);
        assert_eq!(state.binding("one", None), Some(&new));
    }

    #[test]
    fn credential_rotation_and_failed_binds_release_cached_state() {
        let live = Rc::new(Cell::new(0));
        let mut state = State::new();
        for i in 0..100 {
            let b = binding(&format!("password-{i}"));
            let token = state.start_bind("one".into());
            let opened = BTreeMap::from([(b.connection_key(), Socket::new(&live))]);
            assert!(state.commit_bind(token, vec![(None, b)], opened));
            assert_eq!(live.get(), 1);
            assert_eq!(state.connections.len(), 1);
        }
        let failed = state.start_bind("one".into());
        state.cancel_bind(failed);
        assert_eq!(live.get(), 0);
        assert!(state.connections.is_empty());
        assert!(state.workloads.is_empty());
    }

    #[test]
    fn failed_older_bind_cannot_remove_newer_named_bindings() {
        let mut state = State::<Socket>::new();
        let old = state.start_bind("one".into());
        let current = state.start_bind("one".into());
        let default = binding("default");
        let named = binding("named");
        assert!(state.commit_bind(
            current,
            vec![
                (None, default.clone()),
                (Some("orders".into()), named.clone())
            ],
            BTreeMap::new()
        ));
        state.cancel_bind(old);
        assert_eq!(state.binding("one", None), Some(&default));
        assert_eq!(state.binding("one", Some("orders".into())), Some(&named));
        assert!(state.binding("two", Some("orders".into())).is_none());
        bind(&mut state, "one", default.clone());
        assert!(state.binding("one", Some("orders".into())).is_none());
        assert_eq!(state.binding("one", None), Some(&default));
    }
}
