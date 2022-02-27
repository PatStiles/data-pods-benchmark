use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use crate::objects::*;

/// Keep track of all types of different applications
#[derive(Default)]
pub struct TypeRegistry {
    mapping: Mutex<HashMap<String, ObjectTypeList>>,
}

impl TypeRegistry {
    pub fn register_types(
        &self,
        application: String,
        type_map: BTreeMap<String, (u16, ObjectFieldMap)>,
    ) {
        let mut types = ObjectTypeList::new();
        for (_, (typeid, fields)) in type_map.iter() {
            let t = ObjectType::new(application.clone(), *typeid, fields.clone());
            types.push(Arc::new(t));
        }

        let mut mapping = self.mapping.lock().unwrap();
        if mapping.insert(application.clone(), types).is_some() {
            panic!("Types for application {} already existed", application);
        }
    }

    pub fn get(&self, application: &str, id: ObjectTypeId) -> Arc<ObjectType> {
        let mapping = self.mapping.lock().unwrap();

        let app = match mapping.get(application) {
            Some(a) => a,
            _ => {
                panic!("No such application: {}", application);
            }
        };

        let idx = id as usize;

        if idx >= app.len() {
            panic!("Type index out of bounds!");
        }

        app[idx].clone()
    }
}
