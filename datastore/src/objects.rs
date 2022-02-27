use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use cowlang::TypeDefinition;
use rand::random;
use serde::{Deserialize, Serialize};

use byteorder::{ByteOrder, LittleEndian};
use serde_bytes::ByteBuf;

use blockchain_simulator::AccountId;

use byte_slice_cast::AsByteSlice;

use crate::merkle::MerkleHash;
use crate::values::Value;

use sha2::{Digest, Sha512};

pub type ObjectTypeId = u16;

pub type ObjectUid = u64;
pub type FieldUid = ObjectUid;

#[derive(Serialize, Deserialize, Hash, Clone, Debug, PartialEq, Eq)]
pub struct ObjectId {
    uid: ObjectUid,
    location: AccountId,
}

type ObjectFieldOffset = usize;
pub type ObjectFieldMap = BTreeMap<String, (ObjectFieldOffset, TypeDefinition)>;

pub struct ObjectType {
    //FIXME change application to 64bit id
    application: String,
    typeid: ObjectTypeId,
    field_map: Arc<ObjectFieldMap>,
}

#[derive(Clone)]
pub struct Object {
    //TODO add owner information
    application: String,
    owner: AccountId,
    typeid: ObjectTypeId,

    field_map: Arc<ObjectFieldMap>,
    field_values: Vec<Value>,
}

impl TryFrom<Value> for ObjectId {
    type Error = ();

    fn try_from(v: Value) -> Result<ObjectId, Self::Error> {
        let bytes: ByteBuf = match v.try_into() {
            Ok(b) => b,
            Err(_) => {
                return Err(());
            }
        };

        if bytes.len() != 16 {
            return Err(());
        }

        let uid = LittleEndian::read_u64(&bytes[0..8]);
        let location = LittleEndian::read_u64(&bytes[8..16]);

        Ok(ObjectId { uid, location })
    }
}

#[allow(clippy::from_over_into)] //TODO
impl Into<Value> for ObjectId {
    fn into(self) -> Value {
        let mut buf = vec![0; 16];
        LittleEndian::write_u64(&mut buf[0..8], self.uid);
        LittleEndian::write_u64(&mut buf[8..16], self.location);

        ByteBuf::from(buf).into()
    }
}

pub type ObjectTypeList = Vec<Arc<ObjectType>>;

pub fn random_object_uid() -> ObjectUid {
    random::<ObjectUid>()
}

pub fn parse_path(path: &str) -> Vec<String> {
    path.split('.')
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn field_name_to_uid(name: &str) -> FieldUid {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    hasher.finish()
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.field_map, &other.field_map) {
            self.field_values == other.field_values
        } else {
            false
        }
    }
}

impl fmt::Debug for Object {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Object {{")?;
        for (field_name, (offset, _type)) in self.field_map.iter() {
            if *offset > 0 {
                write!(f, ", {:?}={:?}", &field_name, &self.field_values[*offset])?;
            } else {
                write!(f, "{:?}={:?}", &field_name, &self.field_values[*offset])?;
            }
        }

        write!(f, "}}")
    }
}

impl ObjectId {
    pub fn new(uid: ObjectUid, location: AccountId) -> Self {
        Self { uid, location }
    }
}

impl ObjectId {
    pub fn get_uid(&self) -> ObjectUid {
        self.uid
    }

    pub fn get_location(&self) -> AccountId {
        self.location
    }

    pub fn is_local(&self, location: AccountId) -> bool {
        self.location == location
    }
}

impl Object {
    pub fn clone_as_value(&self) -> Value {
        let mut result = Value::make_map();

        for (pos, field_name) in self.field_map.keys().enumerate() {
            result
                .map_insert(field_name.clone(), self.field_values[pos].clone())
                .unwrap();
        }

        result
    }

    pub fn get_hash(&self) -> MerkleHash {
        let mut hasher = Sha512::new();

        hasher.update(&self.application);
        hasher.update(&[self.typeid as u64, self.owner].as_byte_slice());

        for val in self.field_values.iter() {
            val.hash(&mut hasher);
        }

        hasher.finalize()
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self.field_map.get(key) {
            Some((offset, _)) => self.field_values.get(*offset),
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        match self.field_map.get(key) {
            Some((offset, _)) => self.field_values.get_mut(*offset),
            None => None,
        }
    }

    pub fn get_owner(&self) -> AccountId {
        self.owner
    }

    pub fn get_typeid(&self) -> ObjectTypeId {
        self.typeid
    }

    pub fn get_application(&self) -> &str {
        &self.application
    }
}

impl ObjectType {
    pub fn new(application: String, typeid: ObjectTypeId, field_map: ObjectFieldMap) -> Self {
        Self {
            application,
            typeid,
            field_map: Arc::new(field_map),
        }
    }

    pub fn create_object(
        &self,
        owner: AccountId,
        mut named_fields: HashMap<String, Value>,
    ) -> Object {
        if self.field_map.len() != named_fields.len() {
            panic!("Types don't match!");
        }

        let mut field_values = Vec::new();

        for (field_name, _field_type) in self.field_map.iter() {
            match named_fields.remove(field_name) {
                Some(value) => {
                    field_values.push(value);
                }
                None => {
                    panic!("Missing field");
                }
            }
        }

        Object {
            application: self.application.clone(),
            owner,
            typeid: self.typeid,
            field_map: self.field_map.clone(),
            field_values,
        }
    }

    pub fn value_to_object(&self, owner: AccountId, mut named_fields: Value) -> Object {
        if self.field_map.len() != named_fields.num_children() {
            panic!("Types don't match!");
        }

        let mut field_values = Vec::new();

        for (field_name, _field_type) in self.field_map.iter() {
            match named_fields.remove(field_name) {
                Ok(value) => {
                    field_values.push(value);
                }
                Err(e) => {
                    panic!("Got unexpected error: {:?}", e);
                }
            }
        }

        Object {
            application: self.application.clone(),
            owner,
            typeid: self.typeid,
            field_map: self.field_map.clone(),
            field_values,
        }
    }

    pub fn get_key_pos(&self, key: &str) -> Option<usize> {
        for (pos, k) in self.field_map.keys().enumerate() {
            if k == key {
                return Some(pos);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use cowlang::PrimitiveType;
    use std::convert::{Into, TryInto};

    #[test]
    fn is_local() {
        let loc1: AccountId = 14572614;
        let loc2: AccountId = 61725261;

        let oid = ObjectId::new(145143, loc1);

        assert_eq!(true, oid.is_local(loc1));
        assert_eq!(false, oid.is_local(loc2));
    }

    #[test]
    fn object_eq() {
        let mut fields = ObjectFieldMap::new();
        fields.insert(
            "value".to_string(),
            (0, TypeDefinition::Primitive(PrimitiveType::String)),
        );

        let app = "testapp".to_string();
        let field_map = Arc::new(fields);

        let typeid = 0;
        let owner: AccountId = 42;

        let obj1 = Object {
            application: app.clone(),
            owner,
            typeid,
            field_map: field_map.clone(),
            field_values: vec!["foo".into()],
        };
        let obj2 = Object {
            application: app.clone(),
            owner,
            typeid,
            field_map: field_map.clone(),
            field_values: vec!["bar".into()],
        };
        let obj3 = Object {
            application: app.clone(),
            owner,
            typeid,
            field_map: field_map.clone(),
            field_values: vec!["bar".into()],
        };

        assert_ne!(&obj1, &obj2);
        assert_eq!(&obj2, &obj3);
    }

    #[test]
    fn convert_object_id() {
        let obj_id = ObjectId::new(55131, 11151);

        let val: Value = obj_id.clone().into();

        let obj_id2: ObjectId = val.try_into().unwrap();

        assert_eq!(obj_id, obj_id2);
    }
}
