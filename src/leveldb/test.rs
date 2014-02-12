extern mod leveldb;

use std::str::from_utf8;

use leveldb::DB;
use leveldb::options;

#[test]
fn test_db_open() {
    let res = DB::open("db", [options::CREATE_IF_MISSING]);
    match res {
        Ok(db) => {
            let mut it = db.iter([]);
            for (key, value) in it {
                println!("key: {}, val: {}", from_utf8(key), from_utf8(value));
            }
            let res = db.put("foo".as_bytes(), "bar".as_bytes(), []);
            match res {
                Ok(_) => {},
                Err(err) => fail!(err)
            }
            let res = db.get("foo".as_bytes(), []);
            match res {
                Ok(val) => println!("{}", from_utf8(val)),
                Err(err) => fail!(err)
            }
            db.close();
        },
        Err(err) => {
            fail!(err);
        },
    }
}
