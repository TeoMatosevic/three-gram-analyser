pub fn parse_time_from_line_select(line: &str) -> Option<f64> {
    line.split("seconds")
        .next()?
        .split(": ")
        .nth(1)?
        .trim()
        .parse::<f64>()
        .ok()
}
pub fn parse_time_from_line_insert(line: &str) -> Option<f64> {
    line.split(" ").nth(8).and_then(|s| s.parse::<f64>().ok())
}
pub fn calculate_std_dev(times: &[f64], mean: f64) -> f64 {
    let variance = times
        .iter()
        .map(|&time| {
            let diff = mean - time;
            diff * diff
        })
        .sum::<f64>()
        / times.len() as f64;

    variance.sqrt()
}
pub fn calculate_median(times: &mut [f64]) -> f64 {
    times.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let mid = times.len() / 2;
    if times.len() % 2 == 0 {
        (times[mid - 1] + times[mid]) / 2.0
    } else {
        times[mid]
    }
}
pub fn calculate_min_max(times: &[f64]) -> (f64, f64) {
    times
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), &val| {
            (min.min(val), max.max(val))
        })
}
pub fn calculate_percentile(times: &[f64], percentile: u32) -> f64 {
    let mut sorted_times = times.to_vec();
    sorted_times.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
    let index = (percentile as f64 / 100.0 * (sorted_times.len() - 1) as f64).round() as usize;
    sorted_times[index]
}
