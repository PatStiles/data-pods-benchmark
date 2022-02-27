use std::net::{SocketAddr, ToSocketAddrs};

use serde::Serialize;

pub mod enclave_protocol;

// "Hash Set" that uses a vector as storage
// More memory efficient but O(n) insertions

#[derive(Serialize, Default)]
pub struct VecSet<ValueType> {
    inner: Vec<ValueType>,
}

impl<ValueType: std::cmp::Ord> VecSet<ValueType> {
    pub fn insert(&mut self, val: ValueType) -> bool {
        match self.inner.binary_search(&val) {
            Ok(_) => false,
            Err(pos) => {
                self.inner.insert(pos, val);
                true
            }
        }
    }

    pub fn contains(&self, val: &ValueType) -> bool {
        self.inner.binary_search(val).is_ok()
    }

    pub fn get_position(&self, val: &ValueType) -> Option<usize> {
        match self.inner.binary_search(val) {
            Ok(pos) => Some(pos),
            Err(_) => None,
        }
    }

    pub fn remove(&mut self, val: &ValueType) -> bool {
        match self.inner.binary_search(val) {
            Ok(pos) => {
                self.inner.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    pub fn iter(&self) -> core::slice::Iter<ValueType> {
        self.inner.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

#[derive(Serialize, Default)]
pub struct VecMultiSet<ValueType> {
    inner: Vec<ValueType>,
}

impl<ValueType: std::cmp::Ord> VecMultiSet<ValueType> {
    pub fn insert(&mut self, val: ValueType) {
        let pos = match self.inner.binary_search(&val) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };

        self.inner.insert(pos, val);
    }

    pub fn contains(&self, val: &ValueType) -> bool {
        self.inner.binary_search(val).is_ok()
    }

    pub fn remove(&mut self, val: &ValueType) -> bool {
        match self.inner.binary_search(val) {
            Ok(pos) => {
                self.inner.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    pub fn iter(&self) -> core::slice::Iter<ValueType> {
        self.inner.iter()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

pub fn parse_address(addr_str: &str, default_port: u16) -> SocketAddr {
    if addr_str.contains(':') {
        addr_str.to_socket_addrs().unwrap().next().unwrap()
    } else {
        let addr_str = format!("{addr_str}:{default_port}");
        addr_str.to_socket_addrs().unwrap().next().unwrap()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn insert_remove() {
        let val: i32 = 513;

        let mut set = VecSet::default();

        assert_eq!(true, set.insert(val));

        assert_eq!(false, set.insert(val));
        assert_eq!(true, set.contains(&val));

        assert_eq!(true, set.remove(&val));
        assert_eq!(false, set.contains(&val));
    }

    #[test]
    fn multi_insert_remove() {
        let val: i32 = 513;

        let mut set = VecMultiSet::default();

        set.insert(val);
        set.insert(val);

        assert_eq!(true, set.contains(&val));

        assert_eq!(true, set.remove(&val));
        assert_eq!(true, set.contains(&val));

        assert_eq!(true, set.remove(&val));
        assert_eq!(false, set.contains(&val));
    }
}
