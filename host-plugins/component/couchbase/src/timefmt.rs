//! Calendar formatting for the one place the Data API insists on an absolute
//! instant.
//!
//! Expiry on a write goes out as a Go duration string (`"600s"`), which needs
//! no calendar at all. The `/touch` endpoint is the exception: it takes an
//! ISO 8601 timestamp, so a relative TTL has to be added to the wall clock and
//! rendered as a civil date.
//!
//! Nothing here touches the WIT bindings, so it compiles and tests on the host.

/// Format `unix_seconds` (seconds since 1970-01-01T00:00:00Z) as an ISO 8601
/// instant with millisecond precision, e.g. `2026-07-31T20:42:41.000Z`.
///
/// Millis are always `.000`: the only caller adds a whole number of seconds to
/// a whole-second clock reading, and the Data API documents this shape.
pub fn iso8601_utc(unix_seconds: i64) -> String {
    // Floor-divide so instants before the epoch round the right way; Rust's `/`
    // and `%` truncate toward zero, which would put a negative time-of-day on
    // any pre-epoch second.
    let days = unix_seconds.div_euclid(86_400);
    let secs_of_day = unix_seconds.rem_euclid(86_400);

    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day % 3_600) / 60;
    let second = secs_of_day % 60;

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.000Z")
}

/// Convert a count of days since the Unix epoch into a proleptic-Gregorian
/// `(year, month, day)`.
///
/// This is Howard Hinnant's `civil_from_days`, which works by shifting the year
/// to start in March so the leap day lands at the end of the year and the
/// month-length pattern becomes a simple linear formula. The 719468 constant is
/// the number of days from 0000-03-01 to 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    // A 400-year "era" is exactly 146097 days, so era arithmetic is exact and
    // repeats forever in both directions.
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097); // day of era, [0, 146096]
    // Year of era, [0, 399]. The correction terms remove the century days that
    // are not leap days (and add back the 400-year ones that are).
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of (March-based) year, [0, 365]
    let mp = (5 * doy + 2) / 153; // March-based month, [0, 11]
    let d = doy - (153 * mp + 2) / 5 + 1; // [1, 31]
    let m = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]

    // Shift back to a January-based year: January and February belong to the
    // following calendar year in March-based reckoning.
    let year = if m <= 2 { y + 1 } else { y };
    (year, m as u32, d as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_the_epoch() {
        assert_eq!(iso8601_utc(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn formats_a_known_instant() {
        // 1234567890 is a widely-cited Unix timestamp: 2009-02-13T23:31:30Z.
        assert_eq!(iso8601_utc(1_234_567_890), "2009-02-13T23:31:30.000Z");
    }

    #[test]
    fn handles_leap_days() {
        // 2024 is a leap year, so 2024-02-29 exists.
        assert_eq!(iso8601_utc(1_709_164_800), "2024-02-29T00:00:00.000Z");
        // 2000 was a leap year (divisible by 400) unlike 1900 (divisible by 100).
        assert_eq!(iso8601_utc(951_782_400), "2000-02-29T00:00:00.000Z");
    }

    #[test]
    fn handles_year_boundaries() {
        assert_eq!(iso8601_utc(1_767_225_599), "2025-12-31T23:59:59.000Z");
        assert_eq!(iso8601_utc(1_767_225_600), "2026-01-01T00:00:00.000Z");
    }

    #[test]
    fn handles_pre_epoch_instants() {
        // One second before the epoch must not produce a negative time-of-day.
        assert_eq!(iso8601_utc(-1), "1969-12-31T23:59:59.000Z");
    }

    #[test]
    fn round_trips_every_day_boundary_for_a_century() {
        // Walk day boundaries from 1970 to 2070 and check the calendar advances
        // monotonically and consistently: each step is either the next day of the
        // same month, or the 1st of a later month.
        let (mut py, mut pm, mut pd) = (1970i64, 1u32, 1u32);
        for day in 1..=36_525i64 {
            let (y, m, d) = civil_from_days(day);
            let advanced_within_month = y == py && m == pm && d == pd + 1;
            let rolled_month = (y == py && m == pm + 1 && d == 1) || (y == py + 1 && m == 1 && d == 1);
            assert!(
                advanced_within_month || rolled_month,
                "day {day} produced {y:04}-{m:02}-{d:02} after {py:04}-{pm:02}-{pd:02}"
            );
            (py, pm, pd) = (y, m, d);
        }
        // The last step lands on a date we can name independently: 100 years of
        // 365 days plus the 25 leap days from 1972 through 2068.
        assert_eq!(civil_from_days(100 * 365 + 25), (2070, 1, 1));
    }
}
