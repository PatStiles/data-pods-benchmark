use crate::objects::ObjectUid;

use cowlang::ValueError;

use serde::{Deserialize, Serialize};

use std::fmt;
use std::fmt::Display;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DatastoreError {
    NoSuchFunction { name: String },
    NoSuchObject { uid: ObjectUid },
    InvalidArguments,
    PermissionDenied,
    InvalidKey,
    AlreadyExists { uid: ObjectUid },
    NoSuchField { name: String },
    FieldAlreadyExists,
    TypeMismatch,
    Unexpected,
}

impl From<ValueError> for DatastoreError {
    fn from(e: ValueError) -> Self {
        match e {
            ValueError::FieldAlreadyExists => Self::FieldAlreadyExists,
            ValueError::TypeMismatch => Self::TypeMismatch,
            _ => Self::Unexpected,
        }
    }
}

impl Display for DatastoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self {
            DatastoreError::NoSuchFunction { name } => {
                write!(f, "No such function: {}", name)
            }
            DatastoreError::NoSuchObject { uid } => {
                write!(f, "No such object: {}", uid)
            }
            DatastoreError::InvalidArguments => {
                write!(f, "Invalid arguments")
            }
            DatastoreError::PermissionDenied => {
                write!(f, "Permission denied")
            }
            DatastoreError::InvalidKey => {
                write!(f, "Invalid key")
            }
            DatastoreError::AlreadyExists { uid } => {
                write!(f, "Object {} already exists", uid)
            }
            DatastoreError::TypeMismatch => {
                write!(f, "Type mismatch")
            }
            DatastoreError::FieldAlreadyExists => {
                write!(f, "Field already exists")
            }
            DatastoreError::NoSuchField { name } => {
                write!(f, "No such field '{}'", name)
            }
            DatastoreError::Unexpected => {
                write!(f, "Unexpected error")
            }
        }
    }
}
