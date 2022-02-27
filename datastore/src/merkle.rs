use generic_array::typenum::U64;
use generic_array::GenericArray;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::sync::{Arc, Mutex};

pub type MerkleHash = GenericArray<u8, U64>;

#[derive(Default, Debug)]
pub struct LeafContent {
    hash: MerkleHash,
    dirty: bool,
}

#[derive(Debug)]
pub enum MerkleTree {
    Leaf {
        content: Mutex<LeafContent>,
    },
    Node {
        left_child: Arc<MerkleTree>,
        right_child: Arc<MerkleTree>,
        hash: Mutex<MerkleHash>,
    },
    VirtualNode {
        left_child: Arc<MerkleTree>,
        right_child: Arc<MerkleTree>,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum MerkleProof {
    Leaf {
        hash: MerkleHash,
    },
    Node {
        child: Box<MerkleProof>,
        hashes: Vec<MerkleHash>,
    },
}

/// A read-only version of a Merkle tree
/// Can be used to easily generate proofs and send over the network
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FrozenMerkleTree {
    Leaf {
        hash: MerkleHash,
    },
    Node {
        children: Vec<FrozenMerkleTree>,
        hash: MerkleHash,
    },
}

pub fn get_next_pow2(val: usize) -> usize {
    assert!(val > 0);
    let mut next_pow2 = 1;

    while next_pow2 < val {
        next_pow2 <<= 1;
    }

    next_pow2
}

impl FrozenMerkleTree {
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

        FrozenMerkleTree::new_from_hashes_inner(hashes_out).unwrap()
    }

    pub fn new_from_children(mut children: Vec<FrozenMerkleTree>) -> Self {
        if children.is_empty() {
            panic!("Invalid state");
        } else if children.len() == 1 {
            children.pop().unwrap()
        } else {
            assert!(children.len() % 2 == 0);
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

            FrozenMerkleTree::Node { hash, children }
        }
    }

    fn new_from_hashes_inner(mut hashes: Vec<Option<MerkleHash>>) -> Option<Self> {
        if hashes.is_empty() {
            panic!("Invalid state");
        } else if hashes.len() == 1 {
            let hash = hashes.pop().unwrap();
            hash.map(|hash| FrozenMerkleTree::Leaf { hash })
        } else {
            assert!(hashes.len() % 2 == 0);

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

            Some(FrozenMerkleTree::Node { children, hash })
        }
    }

    /// Get the hash for this particular node or leaf
    pub fn get_hash(&self) -> &MerkleHash {
        match self {
            Self::Leaf { hash } => hash,
            Self::Node { hash, .. } => hash,
        }
    }

    pub fn generate_proof(
        &self,
        position: usize,
        branching_factor: usize,
        set_size: usize,
    ) -> MerkleProof {
        match &*self {
            FrozenMerkleTree::Leaf { hash, .. } => {
                assert!(set_size == 1);
                assert!(position == 0);
                MerkleProof::Leaf { hash: *hash }
            }
            FrozenMerkleTree::Node { children, .. } => {
                assert!(set_size > 1);
                assert!(set_size % branching_factor == 0);

                let subset_size = set_size / branching_factor;
                let mut proof_child = None;
                let mut hashes = Vec::new();

                for (cpos, child) in children.iter().enumerate() {
                    let cstart = cpos * subset_size;
                    let cend = (cpos + 1) * subset_size;

                    if cstart <= position && position < cend {
                        let subpos = position - cstart;
                        let subproof = child.generate_proof(subpos, branching_factor, subset_size);

                        proof_child = Some(subproof);
                    } else {
                        hashes.push(*child.get_hash());
                    }
                }

                MerkleProof::Node {
                    hashes,
                    child: Box::new(proof_child.expect("Merkle proof has invalid state")),
                }
            }
        }
    }
}

impl MerkleTree {
    /// Create a read only copy of this merkle tree
    /// TODO we can be smarter about this and only generate the subtree(s) we actually need...
    pub fn clone_frozen(&self) -> FrozenMerkleTree {
        match self {
            Self::Leaf { content } => {
                let inner = content.lock().unwrap();
                FrozenMerkleTree::Leaf { hash: inner.hash }
            }
            Self::Node {
                left_child,
                right_child,
                hash,
            } => {
                let left = left_child.clone_frozen();
                let right = right_child.clone_frozen();
                let hash = *hash.lock().unwrap();

                let children = vec![left, right];

                FrozenMerkleTree::Node { children, hash }
            }
            Self::VirtualNode {
                left_child,
                right_child,
            } => {
                let left = left_child.clone_frozen();
                let right = right_child.clone_frozen();

                // We have to generate this hash on the fly
                let mut hasher = Sha512::new();
                hasher.update(left.get_hash());
                hasher.update(right.get_hash());

                let hash = hasher.finalize();
                let children = vec![left, right];

                FrozenMerkleTree::Node { children, hash }
            }
        }
    }

    pub fn new_leaf() -> Self {
        let content = LeafContent {
            hash: Sha512::new().finalize(),
            dirty: false,
        };

        MerkleTree::Leaf {
            content: Mutex::new(content),
        }
    }

    pub fn new_node(left_child: Arc<MerkleTree>, right_child: Arc<MerkleTree>) -> Self {
        MerkleTree::Node {
            hash: Mutex::new(Sha512::new().finalize()),
            left_child,
            right_child,
        }
    }

    pub fn new_virtual_node(left_child: Arc<MerkleTree>, right_child: Arc<MerkleTree>) -> Self {
        MerkleTree::VirtualNode {
            left_child,
            right_child,
        }
    }

    pub fn update_hash(&self) -> bool {
        match &*self {
            MerkleTree::Leaf { content } => {
                let mut lock = content.lock().unwrap();

                let was_dirty = lock.dirty;
                lock.dirty = false;

                was_dirty
            }
            MerkleTree::Node {
                left_child,
                right_child,
                hash,
            } => {
                let d1 = right_child.update_hash();
                let d2 = left_child.update_hash();

                let was_dirty = d1 || d2;

                if was_dirty {
                    // recalculate
                    let h1 = right_child.get_hash();
                    let h2 = left_child.get_hash();

                    let mut hasher = Sha512::new();
                    hasher.update(h1);
                    hasher.update(h2);

                    let mut lock = hash.lock().unwrap();
                    *lock = hasher.finalize();
                }

                was_dirty
            }
            MerkleTree::VirtualNode {
                left_child: _,
                right_child: _,
            } => {
                panic!("Virtual nodes cannot cache hashes");
            }
        }
    }

    // The leaf does not have access to the actual data
    // so we need to explicitly update the hash here
    // before calling update_hash on the tree
    pub fn set_hash(&self, new_val: MerkleHash) {
        match &*self {
            MerkleTree::Leaf { content } => {
                let mut lock = content.lock().unwrap();
                lock.hash = new_val;
                lock.dirty = true;
            }
            _ => {
                panic!("Not a leaf!");
            }
        }
    }

    pub fn get_hash(&self) -> MerkleHash {
        match &*self {
            MerkleTree::Leaf { content } => content.lock().unwrap().hash,
            MerkleTree::Node {
                left_child: _,
                right_child: _,
                hash,
            } => *hash.lock().unwrap(),
            MerkleTree::VirtualNode {
                left_child,
                right_child,
            } => {
                // recalculate
                let h1 = right_child.get_hash();
                let h2 = left_child.get_hash();

                let mut hasher = Sha512::new();
                hasher.update(h1);
                hasher.update(h2);

                hasher.finalize()
            }
        }
    }
}

#[cfg(test)]
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

        let tree = FrozenMerkleTree::new_from_hashes(vec![h1, h2, h3, h4, h5]);

        let proof = tree.generate_proof(4, 2, 8);

        let subproof = if let MerkleProof::Node { child, .. } = proof {
            child
        } else {
            panic!("Invalid state");
        };

        let subproof = if let MerkleProof::Node { child, .. } = *subproof {
            child
        } else {
            panic!("Invalid state!");
        };

        let subproof = if let MerkleProof::Node { child, .. } = *subproof {
            child
        } else {
            panic!("Invalid state!");
        };

        assert_eq!(MerkleProof::Leaf { hash: h5 }, *subproof);
    }
}
