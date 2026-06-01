use std::time::Duration;

pub(crate) const DEFAULT_ENDPOINT: &str = "unix:///run/crius/crius.sock";

pub(crate) fn parse_duration(value: &str) -> Result<Duration, String> {
    let trimmed = value.trim();
    let split_at = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, unit) = trimmed.split_at(split_at);

    if digits.is_empty() || unit.is_empty() {
        return Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        ));
    }

    let amount = digits
        .parse::<u64>()
        .map_err(|_| format!("invalid duration \"{value}\": value is out of range"))?;

    match unit {
        "ms" => Ok(Duration::from_millis(amount)),
        "s" => Ok(Duration::from_secs(amount)),
        "m" => amount
            .checked_mul(60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        "h" => amount
            .checked_mul(60 * 60)
            .map(Duration::from_secs)
            .ok_or_else(|| format!("invalid duration \"{value}\": value is out of range")),
        _ => Err(format!(
            "invalid duration \"{value}\": expected an integer followed by ms, s, m, or h"
        )),
    }
}
