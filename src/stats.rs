use crate::query_3_grams::writer;
use chrono::{DateTime, Utc};
use core::fmt::Write;
use glob::glob;
use std::error::Error;
use std::fs;
use std::io::{self, BufRead};

pub mod helpers;

pub fn create_select_stats() -> Result<(), Box<dyn Error>> {
    let mut total_exact_frequency_time = 0.0;
    let mut total_all_values_time = 0.0;
    let mut exact_frequency_times = Vec::new();
    let mut all_values_times = Vec::new();
    let mut count = 0;

    for entry in glob("/home/projekt/query-results/select/*").expect("Failed to read glob pattern")
    {
        match entry {
            Ok(path) => {
                let file = fs::File::open(&path)?;
                let reader = io::BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;
                    if line.starts_with("Time taken to get the exact frequency:") {
                        if let Some(time) = helpers::parse_time_from_line_select(&line) {
                            total_exact_frequency_time += time;
                            exact_frequency_times.push(time);
                        }
                    } else if line.starts_with("Time taken to get all values:") {
                        if let Some(time) = helpers::parse_time_from_line_select(&line) {
                            total_all_values_time += time;
                            all_values_times.push(time);
                        }
                        count += 1;
                        break;
                    }
                }
            }
            Err(e) => println!("Error reading file: {:?}", e),
        }
    }
    if count == 0 {
        return Ok(());
    }

    let average_exact_frequency_time = total_exact_frequency_time / count as f64;
    let average_all_values_time = total_all_values_time / count as f64;

    let std_dev_exact_frequency =
        helpers::calculate_std_dev(&exact_frequency_times, average_exact_frequency_time);
    let std_dev_all_values = helpers::calculate_std_dev(&all_values_times, average_all_values_time);

    let median_exact_frequency_time = helpers::calculate_median(&mut exact_frequency_times);
    let median_all_values_time = helpers::calculate_median(&mut all_values_times);

    let (min_exact_frequency_time, max_exact_frequency_time) =
        helpers::calculate_min_max(&exact_frequency_times);
    let (min_all_values_time, max_all_values_time) = helpers::calculate_min_max(&all_values_times);

    let percentile_90_exact = helpers::calculate_percentile(&exact_frequency_times, 90);
    let percentile_90_all_values = helpers::calculate_percentile(&all_values_times, 90);

    let throughput_exact_frequency = count as f64 / total_exact_frequency_time;
    let throughput_all_values = count as f64 / total_all_values_time;

    let utc: DateTime<Utc> = Utc::now();
    let formated_date_time = utc.format("%Y-%m-%dT%H:%M:%S").to_string();
    let output_file_path = String::from("/home/projekt/stats/select/") + &formated_date_time;

    let mut output: Vec<String> = Vec::new();

    let mut tmp_string = String::new();
    write!(
        &mut tmp_string,
        "Average Time Taken for Exact Frequency: {:.3} seconds (Std Dev: {:.3})",
        average_exact_frequency_time, std_dev_exact_frequency
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Average Time Taken for All Values: {:.3} seconds (Std Dev: {:.3})",
        average_all_values_time, std_dev_all_values
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Median Time Taken for Exact Frequency: {:.3} seconds",
        median_exact_frequency_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Median Time Taken for All Values: {:.3} seconds",
        median_all_values_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Min/Max Time Taken for Exact Frequency: {:.3}/{:.3} seconds",
        min_exact_frequency_time, max_exact_frequency_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Min/Max Time Taken for All Values: {:.3}/{:.3} seconds",
        min_all_values_time, max_all_values_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "90th Percentile Time for Exact Frequency: {:.3} seconds",
        percentile_90_exact
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "90th Percentile Time for All Values: {:.3} seconds",
        percentile_90_all_values
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Throughput for Exact Frequency: {:.3} queries/second",
        throughput_exact_frequency
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Throughput for All Values: {:.3} queries/second",
        throughput_all_values
    )?;
    output.push(tmp_string.clone());

    let option = writer::WriteOptions::FILE(output_file_path.to_string());
    let result = writer::write_stats(option, &output);

    if let Err(e) = result {
        println!("Error writing to file: {:?}", e);
        return Ok(());
    }
    println!("Statistics for SELECT queries: ");
    println!("");
    for line in output {
        println!("{}", line);
    }
    println!("\nThis information can also be found in file:");
    println!("{}", output_file_path);
    Ok(())
}

pub fn create_insert_stats() -> Result<(), Box<dyn Error>> {
    let mut total_exact_frequency_time = 0.0;
    let mut exact_frequency_times = Vec::new();
    let mut count = 0;

    for entry in glob("/home/projekt/query-results/insert/*").expect("Failed to read glob pattern")
    {
        match entry {
            Ok(path) => {
                let file = fs::File::open(&path)?;
                let reader = io::BufReader::new(file);

                for line in reader.lines() {
                    let line = line?;
                    if line.starts_with("Inserted 3-gram:") {
                        if let Some(time) = helpers::parse_time_from_line_insert(&line) {
                            total_exact_frequency_time += time;
                            exact_frequency_times.push(time);
                            count += 1;
                        }
                    }
                }
            }
            Err(e) => println!("Error reading file: {:?}", e),
        }
    }

    let average_exact_frequency_time = total_exact_frequency_time / count as f64;

    let std_dev_exact_frequency =
        helpers::calculate_std_dev(&exact_frequency_times, average_exact_frequency_time);

    let median_exact_frequency_time = helpers::calculate_median(&mut exact_frequency_times);

    let (min_exact_frequency_time, max_exact_frequency_time) =
        helpers::calculate_min_max(&exact_frequency_times);

    let percentile_90_exact = helpers::calculate_percentile(&exact_frequency_times, 90);

    let throughput_exact_frequency = count as f64 / total_exact_frequency_time;

    let utc: DateTime<Utc> = Utc::now();
    let formated_date_time = utc.format("%Y-%m-%dT%H:%M:%S").to_string();
    let output_file_path = String::from("/home/projekt/stats/insert/") + &formated_date_time;

    let mut output: Vec<String> = Vec::new();

    let mut tmp_string = String::new();
    write!(
        &mut tmp_string,
        "Average Time Taken for Exact Frequency: {:.3} seconds (Std Dev: {:.3})",
        average_exact_frequency_time, std_dev_exact_frequency
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Median Time Taken for Exact Frequency: {:.3} seconds",
        median_exact_frequency_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Min/Max Time Taken for Exact Frequency: {:.3}/{:.3} seconds",
        min_exact_frequency_time, max_exact_frequency_time
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "90th Percentile Time for Exact Frequency: {:.3} seconds",
        percentile_90_exact
    )?;
    output.push(tmp_string.clone());
    tmp_string.clear();
    write!(
        &mut tmp_string,
        "Throughput for Exact Frequency: {:.3} queries/second",
        throughput_exact_frequency
    )?;
    output.push(tmp_string.clone());

    let option = writer::WriteOptions::FILE(output_file_path.to_string());
    let result = writer::write_stats(option, &output);

    if let Err(e) = result {
        println!("Error writing to file: {:?}", e);
        return Ok(());
    }
    println!("Statistics for INSERT queries: ");
    println!("");
    for line in output {
        println!("{}", line);
    }
    println!("\nThis information can also be found in file:");
    println!("{}", output_file_path);
    Ok(())
}
