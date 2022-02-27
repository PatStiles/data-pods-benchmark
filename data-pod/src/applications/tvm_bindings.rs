use cowlang_derive::cow_module;

use bytes::Bytes;

use std::convert::{TryFrom, TryInto};

use tvm_runtime::{load_param_dict, GraphExecutor, SystemLibModule};

use ndarray::Array;

//FIXME do not hardcode
const BATCH_SIZE: usize = 10;
const INPUT_DIMENSIONS: usize = 8;

pub struct TvmBindings {
    model_json: String,
    param_bytes: Bytes,
}

impl TvmBindings {
    pub fn new(model_json: String, param_bytes: Bytes) -> Self {
        Self {
            model_json,
            param_bytes,
        }
    }
}

#[cow_module]
impl TvmBindings {
    pub fn infer(&self, input_vec: Value) {
        let input_vec: Vec<f32> = input_vec.try_into().unwrap();

        let x = Array::from_shape_vec((BATCH_SIZE, INPUT_DIMENSIONS), input_vec).unwrap();

        let model = match tvm_runtime::Graph::try_from(&self.model_json) {
            Ok(m) => m,
            Err(e) => {
                panic!("Failed to parse model '{}': {}", e, self.model_json);
            }
        };

        let syslib = SystemLibModule::default();
        let params = load_param_dict(&self.param_bytes).unwrap();

        let mut exec = GraphExecutor::new(model, &syslib).unwrap();

        exec.load_params(params);
        exec.set_input("data", (&x).into());

        exec.run();
    }
}
