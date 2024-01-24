use query_3_grams::specs;
use scylla::{Session, SessionBuilder};
use std::error::Error;
use std::io::{self, Write};

pub mod query_3_grams;
mod reader;
pub mod stats;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let uri = std::env::var("SCYLLA_URI").unwrap_or_else(|_| "127.0.0.1:9042".to_string());

    let session: Session = SessionBuilder::new().known_node(uri).build().await?;

    println!("Pleas choose an action:");
    println!("[1]: Get frequencies for certain three-grams");
    println!("[2]: Insert a three-gram (or increment its frequency)");
    println!("[3]: Get statistics about the speed of queries");
    println!("[4]: Exit");
    print!("> ");
    io::stdout().flush().unwrap();

    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    let input: Result<i32, _> = input.trim().parse();

    if let Ok(input) = input {
        match input {
            1 => {
                println!("\nGet frequencies about a specific three-gram or run a bulk query?");
                println!("[1]: Get frequencies about a specific three-gram");
                println!("[2]: Run a bulk query");
                print!("> ");
                io::stdout().flush().unwrap();

                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();
                let input: Result<i32, _> = input.trim().parse();

                if let Ok(input) = input {
                    match input {
                        1 => {
                            println!("\nYou chose to get data about a specific three-gram");
                            println!("Please enter the three-gram you want to get data about");
                            println!("Example: \"word_1 word_2 word_3\"");
                            print!("> ");
                            io::stdout().flush().unwrap();

                            let mut input = String::new();
                            io::stdin().read_line(&mut input).unwrap();
                            let three_gram = specs::ThreeGramInput::from(input);

                            match three_gram {
                                Ok(three_gram) => {
                                    let result =
                                        query_3_grams::get_3_gram(&session, &three_gram).await?;

                                    println!("\n{:?}", result);
                                    println!("This information can also be found in file:");
                                    println!(
                                        "/home/projekt/query-results/select/{}-{}-{}",
                                        three_gram.word_1, three_gram.word_2, three_gram.word_3
                                    );
                                }
                                Err(e) => {
                                    eprintln!("{}", e);
                                    return Ok(());
                                }
                            }
                        }
                        2 => {
                            println!("\nYou chose to run a bulk query");
                            println!("Default input file is /home/projekt/query-inputs/input");
                            println!("Do you want to use the default input file?");
                            println!("(If you choose no, the file still has to be in the same directory)");
                            println!("[1]: Yes");
                            println!("[2]: No");
                            print!("> ");
                            io::stdout().flush().unwrap();

                            let mut input = String::new();
                            io::stdin().read_line(&mut input).unwrap();
                            let input: Result<i32, _> = input.trim().parse();

                            if let Ok(input) = input {
                                match input {
                                    1 => {
                                        println!("\nYou chose to use the default input file");
                                        let input_path =
                                            String::from("/home/projekt/query-inputs/input");
                                        let vec = reader::read(input_path);
                                        match vec {
                                            Ok(three_grams) => {
                                                let mut three_gram_vector: Vec<
                                                    specs::ThreeGramInput,
                                                > = Vec::new();
                                                for three_gram in three_grams {
                                                    three_gram_vector.push(three_gram);
                                                }
                                                query_3_grams::get_bulk(
                                                    &session,
                                                    &three_gram_vector,
                                                )
                                                .await?;
                                                println!("Results can be found in directory:");
                                                println!("/home/projekt/query-results/select");
                                            }
                                            _ => {
                                                eprintln!("Something went wrong");
                                                ()
                                            }
                                        };
                                    }
                                    2 => {
                                        println!("\nYou chose to use a custom input file");
                                        println!("Please enter the file name");
                                        print!("> ");
                                        io::stdout().flush().unwrap();

                                        let mut input = String::new();
                                        io::stdin().read_line(&mut input).unwrap();
                                        let input_path =
                                            String::from("/home/projekt/query-inputs/")
                                                + input.trim();
                                        let vec = reader::read(input_path);
                                        match vec {
                                            Ok(three_grams) => {
                                                let mut three_gram_vector: Vec<
                                                    specs::ThreeGramInput,
                                                > = Vec::new();
                                                for three_gram in three_grams {
                                                    three_gram_vector.push(three_gram);
                                                }
                                                query_3_grams::get_bulk(
                                                    &session,
                                                    &three_gram_vector,
                                                )
                                                .await?;
                                                println!("\nResults can be found in directory:");
                                                println!("/home/projekt/query-results/select");
                                            }
                                            _ => {
                                                eprintln!("Something went wrong");
                                                ()
                                            }
                                        };
                                    }
                                    _ => {
                                        println!("Invalid input");
                                    }
                                }
                            } else {
                                println!("Invalid input");
                            }
                        }
                        _ => {
                            println!("Invalid input");
                        }
                    }
                }
            }
            2 => {
                println!("\nYou chose to insert a three-gram (or increment its frequency)");
                println!("Please enter the three-gram you want to insert");
                println!("Example: \"word_1 word_2 word_3\"");
                print!("> ");
                io::stdout().flush().unwrap();

                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();
                let three_gram = specs::ThreeGramInput::from(input);

                match three_gram {
                    Ok(three_gram) => {
                        let result = query_3_grams::insert(&session, &three_gram).await?;
                        let file_path = String::from("/home/projekt/query-results/insert/")
                            + &three_gram.word_1
                            + "-"
                            + &three_gram.word_2
                            + "-"
                            + &three_gram.word_3
                            + "-"
                            + &result.freq.to_string();
                        let option = query_3_grams::writer::WriteOptions::FILE(file_path);
                        let res = query_3_grams::writer::write_insert(option, &result);
                        if let Err(err) = res {
                            eprintln!("{}", err);
                            return Ok(());
                        }
                        print!("\n{:?}", result);
                        println!("This information can also be found in file:");
                        println!(
                            "/home/projekt/query-results/insert/{}-{}-{}-{}",
                            three_gram.word_1, three_gram.word_2, three_gram.word_3, result.freq
                        );
                    }
                    Err(e) => {
                        eprintln!("{}", e);
                        return Ok(());
                    }
                }
            }
            3 => {
                println!("\nYou chose to get statistics about the speed of queries");
                println!("Do you want information about insert or select queries?");
                println!("[1]: Insert");
                println!("[2]: Select");
                print!("> ");
                io::stdout().flush().unwrap();

                let mut input = String::new();
                io::stdin().read_line(&mut input).unwrap();
                let input: Result<i32, _> = input.trim().parse();

                if let Ok(input) = input {
                    match input {
                        1 => {
                            println!("\nYou chose to get statistics about insert queries");
                            let result = stats::create_insert_stats();

                            if let Err(err) = result {
                                eprintln!("{}", err);
                                return Ok(());
                            }
                        }
                        2 => {
                            println!("\nYou chose to get statistics about select queries");
                            let result = stats::create_select_stats();

                            if let Err(err) = result {
                                eprintln!("{}", err);
                                return Ok(());
                            }
                        }
                        _ => {
                            println!("Invalid input");
                        }
                    }
                } else {
                    println!("Invalid input");
                }
            }
            4 => {
                println!("Exiting...");
                return Ok(());
            }
            _ => {
                println!("Invalid input");
            }
        }
    } else {
        println!("Invalid input");
    }
    Ok(())
}
