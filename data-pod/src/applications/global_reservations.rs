use crate::applications::GlobalValuesMap;
use data_pods_store::protocol::Operation;

#[derive(Debug, Clone, Default)]
pub struct GlobalReservationSet {
    reservations: Vec<(String, Operation)>,
}

impl GlobalReservationSet {
    pub fn insert(&mut self, key: String, op: Operation) {
        self.reservations.push((key, op));
    }

    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }

    pub fn abort(&mut self, global_vals: &mut GlobalValuesMap) {
        for (key, op) in self.reservations.drain(..) {
            Self::abort_operation(key, op, global_vals);
        }
    }

    #[inline]
    fn abort_operation(_key: String, _op: Operation, _global_vals: &mut GlobalValuesMap) {
        //TODO
    }

    pub fn commit(&mut self, global_vals: &mut GlobalValuesMap) {
        if self.reservations.is_empty() {
            panic!("Reservation set is empty");
        }

        for (key, op) in self.reservations.drain(..) {
            Self::commit_operation(key, op, global_vals);
        }
    }

    #[inline]
    fn commit_operation(key: String, op: Operation, global_vals: &mut GlobalValuesMap) {
        match op {
            Operation::MapInsert { mut path, arg } => {
                let val = global_vals.get_mut(&key).unwrap();

                if path.len() > 1 {
                    todo!();
                } else {
                    let field = path.drain(..).next().unwrap();
                    val.map_insert(field, arg).unwrap();
                }
            }
            _ => todo!(),
        }
    }
}
