type RecSystem ( model: String, parameters: Bytes )

pub fn create_model(model: String, parameters: Bytes):
    return db.new("RecSystem", { "model": model, "parameters":  parameters})

pub fn infer(mod_id: ObjectId, input_vec: List<f64>):
    let model = db.load_tvm_model(mod_id)
    return model.infer(input_vec)
