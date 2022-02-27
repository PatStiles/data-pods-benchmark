type Bird ( generation: u64, dna: List<u8>)

pub fn create_random_bird():
    let dna = []

    for i in range(0, 32):
        dna.append(random.randint(0, 256) as u8)

    return db.new("Bird", {"generation": 1u, "dna": dna})

fn get_parent(identifier: Bytes):
    return db.get(identifier)

fn create_child(parent1: ObjectId, parent2: ObjectId):
    let dna = []

    for i in range(0, 32):
        if random.randint(0,1) == 0:
            dna.append(parent1["dna"][i])
        else:
            dna.append(parent2["dna"][i])

    let generation = (max(parent1["generation"], parent2["generation"]) + 1) as u64

    return db.new("Bird", {"generation": generation , "dna": dna})

pub meta_fn breed(parent_id1: ObjectId, parent_id2: ObjectId):
    stage:
        assert parent_id1 != parent_id2
        call get_parent(parent_id1) as parent1
        call get_parent(parent_id2) as parent2

    stage:
        call create_child(parent1, parent2) as result

