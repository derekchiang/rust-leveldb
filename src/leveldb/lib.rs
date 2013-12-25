#[link(name = "leveldb",
       uuid = "60bccde2-3c87-4630-9de3-3ce7c9e6328d",
       url = "https://github.com/derekchiang/rust-leveldb")];

#[crate_id = "leveldb"];

#[comment = "A LevelDB binding in Rust."];
#[license = "MIT"];
#[crate_type = "lib"];

// #[deny(non_camel_case_types)];
// #[deny(missing_doc)];

#[feature(macro_rules)];
#[feature(globs)];

use std::ptr::{mut_null, to_unsafe_ptr, to_mut_unsafe_ptr, is_null, is_not_null};
use std::str::raw::from_c_str;
use std::libc::{c_char, size_t};
use std::cast::transmute;
use std::c_str::CString;
use std::vec::raw::from_buf_raw;

use self::cleveldb::*;
use self::options::*;

mod cleveldb;

pub mod options {
    pub enum OpenOption {
        CREATE_IF_MISSING,
        ERROR_IF_EXISTS,
        PARONOID_CHECKS,
    }

    pub enum WriteOption {
        SYNC,
    }

    pub enum ReadOption {
        VERIFY_CHECKSUM,
        FILL_CACHE,
        // USE_SNAPSHOT(),
    }
}

/// A database object
pub struct DB {
    db: *mut leveldb_t
}

pub type error = ~str;

impl DB {
    /// Open a database connection
    pub fn open(name: &str, options: &[OpenOption]) -> Result<~DB, error> {
        unsafe {
            let c_options = leveldb_options_create();
            for option in options.iter() {
                match *option {
                    CREATE_IF_MISSING => {
                        leveldb_options_set_create_if_missing(c_options, 1u8);
                    },
                    _ => {}
                }
            }
            let mut err: *mut c_char = mut_null();
            let c_db = leveldb_open(c_options as *leveldb_options_t,
                name.to_c_str().unwrap(),
                to_mut_unsafe_ptr(&mut err));
            if is_null(c_db) {
                return Err(from_c_str(err as *c_char));
            } else {
                return Ok(~DB{
                    db: c_db
                });
            }
        }
    }

    pub fn close(&self) {
        unsafe {
            leveldb_close(self.db);
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8], options: &[WriteOption]) -> Result<(), error> {
        unsafe {
            let c_options = leveldb_writeoptions_create();
            for option in options.iter() {
                match *option {
                    SYNC => {
                        leveldb_writeoptions_set_sync(c_options, 1u8);
                    }
                }
            }
            let mut c_err: *mut c_char = mut_null();
            let c_key = key.to_c_str();
            let c_key_len = c_key.len();
            let c_value = value.to_c_str();
            let c_value_len = c_value.len();
            leveldb_put(self.db, c_options as *leveldb_writeoptions_t,
                c_key.unwrap(), c_key_len as size_t,
                c_value.unwrap(), c_value_len as size_t,
                to_mut_unsafe_ptr(&mut c_err));
            if is_not_null(c_err) {
                return Err(from_c_str(c_err as *c_char));
            } else {
                return Ok(());
            }
        }
    }

    pub fn get(&self, key: &[u8], options: &[ReadOption]) -> Result<~[u8], error> {
        unsafe {
            let c_options = leveldb_readoptions_create();
            for option in options.iter() {
                match *option {
                    VERIFY_CHECKSUM => {
                        leveldb_readoptions_set_verify_checksums(c_options, 1u8);
                    },
                    FILL_CACHE => {
                        leveldb_readoptions_set_fill_cache(c_options, 1u8);
                    }
                }
            }
            let mut c_err: *mut c_char = mut_null();
            let c_key = key.to_c_str();
            let c_key_len = c_key.len();
            let mut c_value_len: size_t = 0;
            let c_value = leveldb_get(self.db, c_options as *leveldb_readoptions_t,
                c_key.unwrap(), c_key_len as size_t,
                to_mut_unsafe_ptr(&mut c_value_len),
                to_mut_unsafe_ptr(&mut c_err));
            if is_not_null(c_err) {
                return Err(from_c_str(c_err as *c_char));
            } else {
                return Ok(from_buf_raw(c_value as *u8, c_value_len as uint));
            }
        }
    }
}