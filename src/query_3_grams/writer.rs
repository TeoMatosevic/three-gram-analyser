use super::specs::ThreeGramGetResult;
use super::specs::ThreeGramInsertResult;
use std::fs::File;
use std::io::{self, Write};

pub enum WriteOptions {
    FILE(String),
}

pub fn write_three_gram(option: WriteOptions, three_gram: &ThreeGramGetResult) -> Result<(), io::Error> {
    match option {
        WriteOptions::FILE(file_name) => {
            let mut file = File::create(file_name)?;
            write!(file, "{:?}", three_gram)?;
            Ok(())
        }
    }
}

pub fn write_stats(option: WriteOptions, stats: &Vec<String>) -> Result<(), io::Error> {
    match option {
        WriteOptions::FILE(file_name) => {
            let mut file = File::create(file_name)?;
            for stat in stats {
                write!(file, "{}\n", stat)?;
            }
            Ok(())
        }
    }
}

pub fn write_insert(option: WriteOptions, insert: &ThreeGramInsertResult) -> Result<(), io::Error> {
    match option {
        WriteOptions::FILE(file_name) => {
            let mut file = File::create(file_name)?;
            write!(file, "{:?}", insert)?;
            Ok(())
        }
    }
}
