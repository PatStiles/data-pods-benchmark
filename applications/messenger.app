type Account ( messages: Map<String, List<String>> )

pub fn create_account(name: String):
    return db.new("Account", { "messages": {} })

pub fn count_messages(src: ObjectId):
    assert.owns_object(src)

    let chats = db.get_field(src, ["messages"])
    let result = 0

    for msgs in chats.values():
        result += msgs.len()

    return result

fn add_message(account: ObjectId, conv: ObjectId, msg: String):
    db.list_append(account, ["messages", str(conv)], msg)

fn prepare_message(src: ObjectId):
    assert.owns_object(src)

pub meta_fn send_message(src: ObjectId, dst: ObjectId, msg: String):
    stage:
        assert src != dst
        call prepare_message(src)

    stage:
        call add_message(src, dst, msg)
        call add_message(dst, src, msg)
