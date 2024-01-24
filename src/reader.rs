use crate::query_3_grams::specs;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn process_line(line: String, vec: &mut Vec<specs::ThreeGramInput>) -> Result<(), Box<dyn Error>> {
    let words: Vec<&str> = line.trim().split_whitespace().collect();

    let word_1 = String::from(words[0]);
    let word_2 = String::from(words[1]);
    let word_3 = String::from(words[2]);

    let three_gram = specs::ThreeGramInput::new(word_1, word_2, word_3);

    vec.push(three_gram);

    Ok(())
}

pub fn read(path: String) -> Result<Vec<specs::ThreeGramInput>, Box<dyn Error>> {
    let mut three_gram_vec: Vec<specs::ThreeGramInput> = Vec::new();
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    for line in reader.lines() {
        match line {
            Ok(line) => {
                if let Err(err) = process_line(line, &mut three_gram_vec) {
                    return Err(err);
                }
            }
            Err(err) => return Err(Box::new(err)),
        };
    }

    Ok(three_gram_vec)
}

