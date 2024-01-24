use core::fmt::{self, Write};
use itertools::Itertools;
use std::collections::HashMap;
use std::time::Duration;

static DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT: usize = 10;

pub struct ThreeGram {
    pub word_1: String,
    pub word_2: String,
    pub word_3: String,
    pub freq: i32,
}

pub struct ThreeGramInput {
    pub word_1: String,
    pub word_2: String,
    pub word_3: String,
}

pub struct ThreeGramInsertResult {
    pub three_gram_input: ThreeGramInput,
    pub time_taken: Duration,
    pub freq: i32,
}

pub struct WordPair {
    pub word_1: String,
    pub word_2: String,
}

pub struct QueryResult {
    pub word_pair: WordPair,
    pub word_pair_map: HashMap<String, i32>,
}

pub struct ThreeGramGetResult {
    pub three_gram_input: ThreeGramInput,
    pub time_taken_all: Duration,
    pub time_taken_one: Duration,
    pub exact_freq: i32,
    pub result_1_2_pk: Option<QueryResult>,
    pub result_1_3_pk: Option<QueryResult>,
    pub result_2_3_pk: Option<QueryResult>,
}

impl ThreeGram {
    pub fn new(word_1: String, word_2: String, word_3: String, freq: i32) -> ThreeGram {
        ThreeGram {
            word_1,
            word_2,
            word_3,
            freq,
        }
    }
}

impl ThreeGramInput {
    pub fn new(word_1: String, word_2: String, word_3: String) -> ThreeGramInput {
        ThreeGramInput {
            word_1,
            word_2,
            word_3,
        }
    }

    pub fn from(input: String) -> Result<ThreeGramInput, String> {
        let words: Vec<&str> = input.trim().split_whitespace().collect();

        if words.len() != 3 {
            return Err("Input must contain 3 words".to_string());
        }

        let word_1 = String::from(words[0]);
        let word_2 = String::from(words[1]);
        let word_3 = String::from(words[2]);

        Ok(ThreeGramInput::new(word_1, word_2, word_3))
    }
}

impl ThreeGramInsertResult {
    pub fn new(
        three_gram_input: ThreeGramInput,
        time_taken: Duration,
        freq: i32,
    ) -> ThreeGramInsertResult {
        ThreeGramInsertResult {
            three_gram_input,
            time_taken,
            freq,
        }
    }
}

impl WordPair {
    pub fn new(word_1: String, word_2: String) -> WordPair {
        WordPair { word_1, word_2 }
    }
}

impl QueryResult {
    pub fn new(word_pair: WordPair, word_pair_map: HashMap<String, i32>) -> QueryResult {
        QueryResult {
            word_pair,
            word_pair_map,
        }
    }
}

impl ThreeGramGetResult {
    pub fn new(
        three_gram_input: ThreeGramInput,
        time_taken_all: Duration,
        time_taken_one: Duration,
        exact_freq: i32,
        result_1_2_pk: Option<QueryResult>,
        result_1_3_pk: Option<QueryResult>,
        result_2_3_pk: Option<QueryResult>,
    ) -> ThreeGramGetResult {
        ThreeGramGetResult {
            three_gram_input,
            time_taken_all,
            time_taken_one,
            exact_freq,
            result_1_2_pk,
            result_1_3_pk,
            result_2_3_pk,
        }
    }
}

impl fmt::Debug for ThreeGramInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result_string = String::new();
        let first_word = &self.word_1;
        let second_word = &self.word_2;
        let third_word = &self.word_3;
        write!(
            &mut result_string,
            "{} {} {}",
            first_word, second_word, third_word,
        )?;

        write!(f, "{}", result_string)
    }
}

impl fmt::Debug for ThreeGramInsertResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result_string = String::new();
        let first_word = &self.three_gram_input.word_1;
        let second_word = &self.three_gram_input.word_2;
        let third_word = &self.three_gram_input.word_3;
        let time_taken = &self.time_taken;
        let freq = &self.freq;
        write!(
            &mut result_string,
            "Inserted 3-gram: {} {} {} = {} in {}.{:03} seconds\n",
            first_word,
            second_word,
            third_word,
            freq,
            time_taken.as_secs(),
            time_taken.subsec_millis()
        )?;
        write!(f, "{}", result_string)
    }
}

impl fmt::Debug for ThreeGramGetResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut result_string = String::new();
        let first_word_input = &self.three_gram_input.word_1;
        let second_word_input = &self.three_gram_input.word_2;
        let third_word_input = &self.three_gram_input.word_3;
        let time_taken_all = &self.time_taken_all;
        let time_taken_one = &self.time_taken_one;
        let exact_freq = &self.exact_freq;
        write!(
            &mut result_string,
            "Given 3-gram: {} {} {} = {}\n",
            first_word_input, second_word_input, third_word_input, exact_freq
        )?;
        write!(
            &mut result_string,
            "Time taken to get the exact frequency: {}.{:03} seconds\n",
            time_taken_one.as_secs(),
            time_taken_one.subsec_millis()
        )?;
        write!(
            &mut result_string,
            "Time taken to get all values: {}.{:03} seconds\n",
            time_taken_all.as_secs(),
            time_taken_all.subsec_millis()
        )?;
        if let Some(result_1_2_pk) = &self.result_1_2_pk {
            let first_word = &result_1_2_pk.word_pair.word_1;
            let second_word = &result_1_2_pk.word_pair.word_2;
            write!(
                &mut result_string,
                "--- query executed based on first and second word ---\n"
            )?;
            write!(
                &mut result_string,
                "words: {} {} _____\n",
                first_word, second_word
            )?;
            let count = result_1_2_pk.word_pair_map.len();
            let top_10_elements: Vec<_> = result_1_2_pk
                .word_pair_map
                .iter()
                .collect::<Vec<_>>()
                .into_iter()
                .sorted_by(|a, b| b.1.cmp(a.1))
                .take(DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT as usize)
                .collect();
            for (word, frequency) in top_10_elements {
                write!(&mut result_string, " {}: {}\n", word, frequency)?;
            }
            if count > DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT {
                let remaining_count = count - DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT;
                write!(&mut result_string, " ... and {} more\n", remaining_count)?;
            }
        }
        if let Some(result_1_3_pk) = &self.result_1_3_pk {
            let first_word = &result_1_3_pk.word_pair.word_1;
            let third_word = &result_1_3_pk.word_pair.word_2;
            write!(
                &mut result_string,
                "--- query executed based on first and third word ---\n"
            )?;
            write!(
                &mut result_string,
                "words: {} _____ {}\n",
                first_word, third_word
            )?;
            let count = result_1_3_pk.word_pair_map.len();
            let top_10_elements: Vec<_> = result_1_3_pk
                .word_pair_map
                .iter()
                .collect::<Vec<_>>()
                .into_iter()
                .sorted_by(|a, b| b.1.cmp(a.1))
                .take(DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT as usize)
                .collect();
            for (word, frequency) in top_10_elements {
                write!(&mut result_string, " {}: {}\n", word, frequency)?;
            }
            if count > DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT {
                let remaining_count = count - DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT;
                write!(&mut result_string, " ... and {} more\n", remaining_count)?;
            }
        }
        if let Some(result_2_3_pk) = &self.result_2_3_pk {
            let second_word = &result_2_3_pk.word_pair.word_1;
            let third_word = &result_2_3_pk.word_pair.word_2;
            write!(
                &mut result_string,
                "--- query executed based on second and third word ---\n"
            )?;
            write!(
                &mut result_string,
                "words: _____ {} {}\n",
                second_word, third_word
            )?;
            let count = result_2_3_pk.word_pair_map.len();
            let top_10_elements: Vec<_> = result_2_3_pk
                .word_pair_map
                .iter()
                .collect::<Vec<_>>()
                .into_iter()
                .sorted_by(|a, b| b.1.cmp(a.1))
                .take(DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT as usize)
                .collect();
            for (word, frequency) in top_10_elements {
                write!(&mut result_string, " {}: {}\n", word, frequency)?;
            }
            if count > DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT {
                let remaining_count = count - DEFAULT_NUMBER_OF_3_GRAMS_TO_PRINT;
                write!(&mut result_string, " ... and {} more\n", remaining_count)?;
            }
        }
        write!(f, "{}", result_string)
    }
}
