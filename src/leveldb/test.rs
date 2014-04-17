extern crate leveldb;

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
                println!("key: {}, val: {}", from_utf8(key.slice_from(0)), from_utf8(value.slice_from(0)));
            }
            let res = db.put("foo".as_bytes(), "bar".as_bytes(), []);
            match res {
                Ok(_) => {},
                Err(err) => fail!(err)
            }
            let res = db.get("foo".as_bytes(), []);
            match res {
                Ok(val) => println!("{}", from_utf8(val.slice_from(0))),
                Err(err) => fail!(err)
            }
            db.close();
        },
        Err(err) => {
            fail!(err);
        },
    }
}
