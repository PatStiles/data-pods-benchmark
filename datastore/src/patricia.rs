use std::sync::Arc;

use arr_macro::arr;
use bit_vec::BitVec;
use sha2::{Digest, Sha512};

use super::merkle::MerkleHash;

use serde::{Deserialize, Serialize};

pub const CHILDREN_PER_BIGNODE: usize = 32; //FIXME rust bug 64;

const BITS_PER_BRANCH: usize = 4;
pub const CHILDREN_PER_BRANCH: usize = 16;

type FrozenChildEntry = Option<Box<FrozenPatriciaTree>>;
type ChildEntry = Option<Box<PatriciaTree>>;
type ChildArray = [ChildEntry; CHILDREN_PER_BRANCH];

#[derive(Debug)]
pub enum PatriciaTree {
    Leaf {
        hash: MerkleHash,
        dirty: bool,
    },
    Extension {
        path: BitVec,
        child: Box<PatriciaTree>,
    },
    Node {
        children: ChildArray,
        hash: MerkleHash,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum PatriciaProof {
    Leaf {
        hash: MerkleHash,
    },
    Node {
        child: Box<PatriciaProof>,
        hashes: Vec<MerkleHash>,
    },
}

/// A read-only version of a Patricia tree
/// Can be used to easily generate proofs and send over the network
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FrozenPatriciaTree {
    Leaf {
        hash: MerkleHash,
    },
    Extension {
        path: BitVec,
        child: Box<FrozenPatriciaTree>,
    },
    /// Node with higher branching factor
    /// Only used at the top level to allow for more shards
    BigNode {
        children: Arc<[FrozenChildEntry; CHILDREN_PER_BIGNODE]>,
        hash: MerkleHash,
    },
    Node {
        children: [FrozenChildEntry; CHILDREN_PER_BRANCH],
        hash: MerkleHash,
    },
}

impl FrozenPatriciaTree {
    pub fn new_from_children(mut children: Vec<FrozenPatriciaTree>) -> Self {
        assert!(children.len() == CHILDREN_PER_BIGNODE);
        let mut result = arr![None; 32];
        let mut hasher = Sha512::new();

        for (pos, entry) in children.drain(..).enumerate() {
            hasher.update(entry.get_hash());
            result[pos] = Some(Box::new(entry));
        }

        let hash = hasher.finalize();

        FrozenPatriciaTree::BigNode {
            children: Arc::new(result),
            hash,
        }
    }

    /// Get the hash for this particular node or leaf
    pub fn get_hash(&self) -> &MerkleHash {
        match self {
            Self::BigNode { hash, .. } => hash,
            Self::Leaf { hash } => hash,
            Self::Extension { child, .. } => child.get_hash(),
            Self::Node { hash, .. } => hash,
        }
    }
}

/*
impl FrozenPatriciaTree {
    pub fn new_from_hashes(mut hashes: Vec<MerkleHash>) -> Self {

        let next_pow2 = get_next_pow2(hashes.len());

        let extra = next_pow2 - hashes.len();
        let mut hashes_out = Vec::new();

        for h in hashes.drain(..) {
            hashes_out.push(Some(h));
        }

        for _ in 0..extra {
            hashes_out.push(None);
        }

        FrozenPatriciaTree::new_from_hashes_inner(hashes_out).unwrap()
    }

    pub fn new_from_children(mut children: Vec<FrozenPatriciaTree>) -> Self {
        if children.is_empty() {
            panic!("Invalid state");
        } else if children.len() == 1 {
            children.pop().unwrap()
        } else {
            assert!(children.len() % 2 == 0 );
            let len = children.len() / 2;

            let right = children.split_off(len);
            let left = children;

            let left_child = Self::new_from_children(left);
            let right_child = Self::new_from_children(right);

            let mut hasher = Sha512::new();
            hasher.update(left_child.get_hash());
            hasher.update(right_child.get_hash());
            let hash = hasher.finalize();

            let children = vec![left_child, right_child];

            FrozenPatriciaTree::Node{ hash, children }
        }
    }

    fn new_from_hashes_inner(mut hashes: Vec<Option<MerkleHash>>) -> Option<Self> {
        if hashes.is_empty() {
            panic!("Invalid state");
        } else if hashes.len() == 1 {
            let hash = hashes.pop().unwrap();

            if let Some(hash) = hash {
                Some( FrozenPatriciaTree::Leaf{ hash } )
            } else {
                None
            }
        } else {
            assert!(hashes.len() % 2 == 0 );

            let mut has_content = false;
            for h in hashes.iter() {
                if h.is_some() {
                    has_content = true;
                    break;
                }
            }

            if !has_content {
                return None;
            }

            let len = hashes.len() / 2;

            let right = hashes.split_off(len);
            let left = hashes;

            let mut hasher = Sha512::new();
            let mut children = Vec::new();

            if let Some(l) = Self::new_from_hashes_inner(left) {
                hasher.update(l.get_hash());
                children.push(l);
            }

            if let Some(r) = Self::new_from_hashes_inner(right) {
                assert!(!children.is_empty());
                hasher.update(r.get_hash());
                children.push(r);
            }

            let hash = hasher.finalize();

            Some( FrozenPatriciaTree::Node{
                children, hash
            } )
        }
    }

    pub fn generate_proof(&self, position: usize, branching_factor: usize, set_size: usize) -> PatriciaProof {
        match &*self {
            FrozenPatriciaTree::Leaf{hash, ..} => {
                assert!(set_size == 1);
                assert!(position == 0);
                PatriciaProof::Leaf{hash: *hash}
            }
            FrozenPatriciaTree::Node{children, ..} => {
                assert!(set_size > 1);
                assert!(set_size % branching_factor == 0);

                let subset_size = set_size / branching_factor;
                let mut proof_child = None;
                let mut hashes = Vec::new();

                for (cpos, child) in children.iter().enumerate() {
                    let cstart = cpos * subset_size;
                    let cend = (cpos+1) * subset_size;

                    if cstart <= position && position < cend {
                        let subpos = position - cstart;
                        let subproof = child.generate_proof(subpos, branching_factor, subset_size);

                        proof_child = Some(subproof);
                    } else {
                        hashes.push(*child.get_hash());
                    }
                }

                PatriciaProof::Node {
                    hashes, child: Box::new( proof_child.expect("Patricia proof has invalid state") )
                }
            }
        }
    }
}*/

impl PatriciaTree {
    /// Create a read only copy of this Patricia tree
    /// TODO we can be smarter about this and only generate the subtree(s) we actually need...
    pub fn clone_frozen(&self) -> FrozenPatriciaTree {
        match self {
            Self::Leaf { hash, .. } => FrozenPatriciaTree::Leaf { hash: *hash },
            Self::Extension { path, child } => {
                let child = child.clone_frozen();

                FrozenPatriciaTree::Extension {
                    path: path.clone(),
                    child: Box::new(child),
                }
            }
            Self::Node { children, hash } => {
                let mut ccopy = arr![None; 16];

                for (pos, child) in children.iter().enumerate() {
                    match child {
                        Some(c) => {
                            let copy = c.clone_frozen();
                            ccopy[pos] = Some(Box::new(copy));
                        }
                        None => {
                            ccopy[pos] = None;
                        }
                    }
                }

                FrozenPatriciaTree::Node {
                    children: ccopy,
                    hash: *hash,
                }
            }
        }
    }

    pub fn new_node() -> Self {
        let children = arr![None; 16];
        let hash = MerkleHash::default();

        Self::Node { children, hash }
    }

    pub fn update_hash(&mut self) -> bool {
        match &mut *self {
            PatriciaTree::Leaf { dirty, .. } => {
                let was_dirty = *dirty;
                *dirty = false;

                was_dirty
            }
            PatriciaTree::Extension { child, .. } => child.update_hash(),
            PatriciaTree::Node { children, hash } => {
                let mut was_dirty = false;

                for child in children.iter_mut().flatten() {
                    let dirty = child.update_hash();
                    if dirty {
                        was_dirty = true;
                    }
                }

                if was_dirty {
                    // recalculate
                    let mut hasher = Sha512::new();
                    for child in children.iter().flatten() {
                        let hash = child.get_hash();
                        hasher.update(hash);
                    }

                    *hash = hasher.finalize();
                }

                was_dirty
            }
        }
    }

    pub fn set_child_hash(
        tree: PatriciaTree,
        mut path: BitVec,
        new_val: MerkleHash,
    ) -> PatriciaTree {
        match tree {
            PatriciaTree::Extension {
                path: mut epath,
                child: other_child,
            } => {
                let mut split_pos = None;

                for (pos, bit) in epath.iter().enumerate() {
                    let val = path[pos];
                    if val != bit {
                        split_pos = Some(pos);
                        break;
                    }
                }

                if let Some(pos) = split_pos {
                    let offset = pos % BITS_PER_BRANCH;
                    let split_pos = pos - offset;
                    assert!(path.len() == epath.len());
                    assert!(epath.len() % BITS_PER_BRANCH == 0);

                    let mut children = arr![None; 16];

                    let leaf = Box::new(Self::Leaf {
                        hash: new_val,
                        dirty: true,
                    });

                    if split_pos == 0 {
                        let nindex = Self::bitvec_to_key(&mut path);
                        let oindex = Self::bitvec_to_key(&mut epath);

                        if path.is_empty() {
                            children[nindex] = Some(leaf);
                            children[oindex] = Some(other_child);
                        } else {
                            let subpath = path.split_off(0);

                            let ext = Self::Extension {
                                path: subpath,
                                child: leaf,
                            };
                            let other_ext = Self::Extension {
                                path: epath,
                                child: other_child,
                            };

                            children[nindex] = Some(Box::new(ext));
                            children[oindex] = Some(Box::new(other_ext));
                        }
                    } else if split_pos + BITS_PER_BRANCH == path.len() {
                        let mut node_index = path.split_off(split_pos);
                        let mut other_index = epath.split_off(split_pos);

                        let subpath = node_index.split_off(BITS_PER_BRANCH);
                        let other_subpath = other_index.split_off(BITS_PER_BRANCH);

                        let nindex = Self::bitvec_to_key(&mut node_index);
                        let oindex = Self::bitvec_to_key(&mut other_index);

                        let ext = Self::Extension {
                            path: subpath,
                            child: leaf,
                        };
                        let other_ext = Self::Extension {
                            path: other_subpath,
                            child: other_child,
                        };

                        children[nindex] = Some(Box::new(ext));
                        children[oindex] = Some(Box::new(other_ext));
                    } else {
                        let subpath = path.split_off(split_pos + BITS_PER_BRANCH);
                        let other_subpath = epath.split_off(split_pos + BITS_PER_BRANCH);

                        let mut node_index = path.split_off(split_pos);
                        let mut other_index = epath.split_off(split_pos);

                        let nindex = Self::bitvec_to_key(&mut node_index);
                        let oindex = Self::bitvec_to_key(&mut other_index);

                        let ext = Self::Extension {
                            path: subpath,
                            child: leaf,
                        };
                        let other_ext = Self::Extension {
                            path: other_subpath,
                            child: other_child,
                        };

                        children[nindex] = Some(Box::new(ext));
                        children[oindex] = Some(Box::new(other_ext));
                    }

                    let node = Self::Node {
                        children,
                        hash: MerkleHash::default(),
                    };

                    if path.is_empty() {
                        node
                    } else {
                        Self::Extension {
                            path,
                            child: Box::new(node),
                        }
                    }
                } else {
                    path.truncate(epath.len());
                    let child = Self::set_child_hash(*other_child, path, new_val);

                    PatriciaTree::Extension {
                        path: epath,
                        child: Box::new(child),
                    }
                }
            }
            PatriciaTree::Node { mut children, hash } => {
                if path.is_empty() {
                    panic!("Invalid state: Empty path as node");
                }

                if path.len() % BITS_PER_BRANCH != 0 {
                    panic!("Path length not a power of 4");
                }

                let key = Self::bitvec_to_key(&mut path);

                {
                    let child = &mut children[key];

                    if let Some(c) = child.take() {
                        *child = Some(Box::new(Self::set_child_hash(*c, path, new_val)));
                    } else {
                        let leaf = Self::Leaf {
                            dirty: true,
                            hash: new_val,
                        };

                        let c = if path.is_empty() {
                            leaf
                        } else {
                            Self::Extension {
                                path,
                                child: Box::new(leaf),
                            }
                        };

                        *child = Some(Box::new(c));
                    }
                }

                PatriciaTree::Node { children, hash }
            }
            PatriciaTree::Leaf { .. } => {
                if !path.is_empty() {
                    panic!("Invalid state: Non-empty path at leaf");
                }

                PatriciaTree::Leaf {
                    hash: new_val,
                    dirty: true,
                }
            }
        }
    }

    fn bitvec_to_key(path: &mut BitVec) -> usize {
        let path = if path.len() == BITS_PER_BRANCH {
            path.split_off(0)
        } else {
            let mut new_path = path.split_off(BITS_PER_BRANCH);
            std::mem::swap(&mut new_path, path);

            new_path
        };

        let mut key: usize = 0;

        for bit in path {
            key += 2 ^ (bit as usize);
        }

        key
    }

    pub fn get_child_hash(&self, mut path: BitVec) -> MerkleHash {
        match &self {
            PatriciaTree::Node { children, .. } => {
                if path.is_empty() {
                    panic!("Invalid state: Empty path as node");
                }

                if path.len() % BITS_PER_BRANCH != 0 {
                    panic!("Path length not a power of 4");
                }

                let key = Self::bitvec_to_key(&mut path);

                if let Some(c) = &children[key] {
                    c.get_child_hash(path)
                } else {
                    panic!("No such child");
                }
            }
            PatriciaTree::Extension { path: epath, child } => {
                assert!(epath.len() <= path.len());
                let mut subpath = path.split_off(epath.len());
                std::mem::swap(&mut path, &mut subpath);

                for (pos, bit) in epath.iter().enumerate() {
                    if subpath[pos] != bit {
                        panic!("No such child!");
                    }
                }

                child.get_child_hash(path)
            }
            PatriciaTree::Leaf { hash, .. } => {
                if !path.is_empty() {
                    panic!("Invalid state: Non-empty path at leaf");
                }

                *hash
            }
        }
    }

    pub fn get_hash(&self) -> MerkleHash {
        match &*self {
            PatriciaTree::Leaf { hash, .. } => *hash,
            PatriciaTree::Extension { child, .. } => child.get_hash(),
            PatriciaTree::Node { hash, .. } => *hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bit_vec::BitVec;

    #[test]
    fn split_extension() {
        let hpath = MerkleHash::default();
        let path1 = BitVec::from_bytes(&hpath);
        let mut path2 = path1.clone();
        path2.set(5, true);

        let hash1 = MerkleHash::default();
        let mut hash2 = hash1.clone();
        hash2[10] = 131;

        let tree = PatriciaTree::new_node();

        let tree = PatriciaTree::set_child_hash(tree, path1.clone(), hash1.clone());

        assert_eq!(tree.get_child_hash(path1.clone()), hash1.clone());

        let tree = PatriciaTree::set_child_hash(tree, path2.clone(), hash2.clone());

        assert_eq!(tree.get_child_hash(path1.clone()), hash1);
        assert_eq!(tree.get_child_hash(path2.clone()), hash2);

        let mut hash3 = hash2.clone();
        hash3[12] = 99;

        let mut path3 = path2.clone();
        path3.set(10, true);

        let tree = PatriciaTree::set_child_hash(tree, path3.clone(), hash3.clone());

        assert_eq!(tree.get_child_hash(path1), hash1);
        assert_eq!(tree.get_child_hash(path2), hash2);
        assert_eq!(tree.get_child_hash(path3), hash3);
    }
}

/* TODO
#[ cfg(test) ]
mod tests {
    use super::*;

    #[test]
    fn next_pow2() {
        assert_eq!(4, get_next_pow2(3));
        assert_eq!(8, get_next_pow2(8));
    }

    #[test]
    fn merkle_proof() {
        let h1 = MerkleHash::default();
        let h2 = MerkleHash::default();
        let h3 = MerkleHash::default();
        let h4 = MerkleHash::default();
        let h5 = MerkleHash::default();

        let tree = FrozenPatriciaTree::new_from_hashes(vec![h1, h2, h3, h4, h5]);

        let proof = tree.generate_proof(4, 2, 8);

        let subproof = if let PatriciaProof::Node{child, ..} = proof {
            child
        } else {
            panic!("Invalid state");
        };

        let subproof = if let PatriciaProof::Node{child, ..} = *subproof {
            child
        } else {
            panic!("Invalid state!");
        };

        let subproof = if let PatriciaProof::Node{child, ..} = *subproof {
            child
        } else {
            panic!("Invalid state!");
        };

        assert_eq!(PatriciaProof::Leaf{hash: h5}, *subproof);
    }
}*/
