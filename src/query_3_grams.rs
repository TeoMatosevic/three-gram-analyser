use crate::Error;
use core::fmt::Write;
use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use scylla::IntoTypedRows;
use std::collections::HashMap;
use std::time::Instant;

pub mod specs;
pub mod writer;

pub async fn get_3_gram(
    session: &scylla::Session,
    input: &specs::ThreeGramInput,
) -> Result<specs::ThreeGramGetResult, Box<dyn Error>> {
    let start_time_one = Instant::now();
    let mut prepared: PreparedStatement = session
                .prepare("SELECT freq FROM n_grams.three_grams_1_2_pk WHERE word_1 = ? AND word_2 = ? AND word_3 = ?")
                .await?;
    prepared.set_consistency(Consistency::One);
    let row = session
        .execute(
            &prepared,
            (
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
            ),
        )
        .await?
        .maybe_first_row_typed::<(i32,)>()?;
    let mut exact_freq: i32 = 0;
    if let Some(row) = row {
        let (freq,): (i32,) = row;
        exact_freq = freq;
    }
    let end_time_one = Instant::now();
    let duration_one = end_time_one - start_time_one;

    let mut map_1_2 = HashMap::new();
    let mut map_1_3 = HashMap::new();
    let mut map_2_3 = HashMap::new();
    let start_time_all = Instant::now();

    let mut prepared: PreparedStatement = session
        .prepare(
            "SELECT word_3, freq FROM n_grams.three_grams_1_2_pk WHERE word_1 = ? AND word_2 = ?",
        )
        .await?;
    prepared.set_consistency(Consistency::One);
    let rows_1_2 = session
        .execute(&prepared, (input.word_1.clone(), input.word_2.clone()))
        .await?
        .rows;

    let mut prepared: PreparedStatement = session
        .prepare(
            "SELECT word_1, freq FROM n_grams.three_grams_2_3_pk WHERE word_2 = ? AND word_3 = ?",
        )
        .await?;
    prepared.set_consistency(Consistency::One);
    let rows_2_3 = session
        .execute(&prepared, (input.word_2.clone(), input.word_3.clone()))
        .await?
        .rows;

    let mut prepared: PreparedStatement = session
        .prepare(
            "SELECT word_2, freq FROM n_grams.three_grams_1_3_pk WHERE word_1 = ? AND word_3 = ?",
        )
        .await?;
    prepared.set_consistency(Consistency::One);
    let rows_1_3 = session
        .execute(&prepared, (input.word_1.clone(), input.word_3.clone()))
        .await?
        .rows;

    let end_time_all = Instant::now();
    let duration_all = end_time_all - start_time_all;

    if let Some(rows) = rows_1_2 {
        for row in rows.into_typed::<(String, i32)>() {
            let (word_3, freq): (String, i32) = row?;
            map_1_2.insert(word_3, freq);
        }
    }
    if let Some(rows) = rows_1_3 {
        for row in rows.into_typed::<(String, i32)>() {
            let (word_2, freq): (String, i32) = row?;
            map_1_3.insert(word_2, freq);
        }
    }
    if let Some(rows) = rows_2_3 {
        for row in rows.into_typed::<(String, i32)>() {
            let (word_1, freq): (String, i32) = row?;
            map_2_3.insert(word_1, freq);
        }
    }
    let three_gram_input = specs::ThreeGramInput::new(
        input.word_1.clone(),
        input.word_2.clone(),
        input.word_3.clone(),
    );
    let result_1_2_pk = Some(specs::QueryResult::new(
        specs::WordPair::new(input.word_1.clone(), input.word_2.clone()),
        map_1_2,
    ));
    let result_1_3_pk = Some(specs::QueryResult::new(
        specs::WordPair::new(input.word_1.clone(), input.word_3.clone()),
        map_1_3,
    ));
    let result_2_3_pk = Some(specs::QueryResult::new(
        specs::WordPair::new(input.word_2.clone(), input.word_3.clone()),
        map_2_3,
    ));
    let result = specs::ThreeGramGetResult {
        three_gram_input,
        time_taken_all: duration_all,
        time_taken_one: duration_one,
        exact_freq,
        result_1_2_pk,
        result_1_3_pk,
        result_2_3_pk,
    };
    let mut absolute_path = String::new();
    write!(
        &mut absolute_path,
        "/home/projekt/query-results/select/{}-{}-{}",
        input.word_1.clone(),
        input.word_2.clone(),
        input.word_3.clone(),
    )?;
    let option = writer::WriteOptions::FILE(absolute_path);
    let res = writer::write_three_gram(option, &result);
    match res {
        Ok(()) => (),
        Err(err) => eprintln!("{}", err),
    }
    Ok(result)
}

pub async fn get_bulk(
    session: &scylla::Session,
    inputs: &Vec<specs::ThreeGramInput>,
) -> Result<(), Box<dyn Error>> {
    for input in inputs {
        _ = get_3_gram(&session, input).await?;
    }
    Ok(())
}

pub async fn insert_new(
    session: &scylla::Session,
    input: &specs::ThreeGramInput,
) -> Result<(), Box<dyn Error>> {
    let mut prepared: PreparedStatement = session
        .prepare(
            "INSERT INTO n_grams.three_grams_1_2_pk (word_1, word_2, word_3, freq) VALUES (?, ?, ?, ?)",
        )
        .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
                1,
            ),
        )
        .await?;

    let mut prepared: PreparedStatement = session
    .prepare(
        "INSERT INTO n_grams.three_grams_1_3_pk (word_1, word_2, word_3, freq) VALUES (?, ?, ?, ?)",
    )
    .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
                1,
            ),
        )
        .await?;

    let mut prepared: PreparedStatement = session
    .prepare(
        "INSERT INTO n_grams.three_grams_2_3_pk (word_1, word_2, word_3, freq) VALUES (?, ?, ?, ?)",
    )
    .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
                1,
            ),
        )
        .await?;

    Ok(())
}

pub async fn update_one(
    session: &scylla::Session,
    input: &specs::ThreeGram,
) -> Result<(), Box<dyn Error>> {
    let freq = input.freq + 1;
    let mut prepared: PreparedStatement = session
    .prepare(
        "UPDATE n_grams.three_grams_1_2_pk SET freq = ? WHERE word_1 = ? AND word_2 = ? AND word_3 = ?",
    )
    .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                freq,
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
            ),
        )
        .await?;

    let mut prepared: PreparedStatement = session
    .prepare(
        "UPDATE n_grams.three_grams_1_3_pk SET freq = ? WHERE word_1 = ? AND word_2 = ? AND word_3 = ?",
    )
    .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                freq,
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
            ),
        )
        .await?;

    let mut prepared: PreparedStatement = session
    .prepare(
        "UPDATE n_grams.three_grams_2_3_pk SET freq = ? WHERE word_1 = ? AND word_2 = ? AND word_3 = ?",
    )
    .await?;
    prepared.set_consistency(Consistency::One);
    session
        .execute(
            &prepared,
            (
                freq,
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
            ),
        )
        .await?;
    Ok(())
}

pub async fn insert(
    session: &scylla::Session,
    input: &specs::ThreeGramInput,
) -> Result<specs::ThreeGramInsertResult, Box<dyn Error>> {
    let start_time = Instant::now();
    let mut prepared: PreparedStatement = session.prepare(
    "SELECT * FROM n_grams.three_grams_1_2_pk WHERE word_1 = ? AND word_2 = ? AND word_3 = ?",
).await?;
    prepared.set_consistency(Consistency::One);
    let row = session
        .execute(
            &prepared,
            (
                input.word_1.clone(),
                input.word_2.clone(),
                input.word_3.clone(),
            ),
        )
        .await?
        .maybe_first_row_typed::<(String, String, String, i32)>()?;

    let three_gram_input = specs::ThreeGramInput::new(
        input.word_1.clone(),
        input.word_2.clone(),
        input.word_3.clone(),
    );

    if let Some(row) = row {
        let (word_1, word_2, word_3, freq): (String, String, String, i32) = row;
        let three_gram = specs::ThreeGram::new(word_1, word_2, word_3, freq);
        update_one(&session, &three_gram).await?;
        let end_time = Instant::now();
        let duration = end_time - start_time;
        return Ok(specs::ThreeGramInsertResult::new(
            three_gram_input,
            duration,
            freq + 1,
        ));
    } else {
        insert_new(&session, &input).await?;
        let end_time = Instant::now();
        let duration = end_time - start_time;
        return Ok(specs::ThreeGramInsertResult::new(
            three_gram_input,
            duration,
            1,
        ));
    }
}
