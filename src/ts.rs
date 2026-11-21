//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 epoch milliseconds (ASCII digits only). Anything else is bad (None).

pub const SECOND: i64 = 1000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline(always)]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline(always)]
fn dim(y: i64, m: i64) -> i64 {
    const D: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && is_leap(y) {
        29
    } else {
        D[m as usize]
    }
}

#[inline(always)]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a > 9 || b > 9 {
        None
    } else {
        Some((a as i64) * 10 + b as i64)
    }
}

/// Seconds since the epoch of the `YYYY-MM-DDTHH:MM:SS` prefix (19 bytes).
#[inline(always)]
fn civil_prefix(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if mo < 1 || mo > 12 || d < 1 || d > dim(y, mo) || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se)
}

/// Milliseconds since the epoch, or None for a bad timestamp. An F3 value too
/// large for i64 is returned as None (it is outside every window anyway).
#[inline]
pub fn parse(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 20 && s[19] == b'Z' {
        return civil_prefix(s).map(|t| t * SECOND);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') {
        let t = civil_prefix(s)?;
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60;
        return Some(if s[19] == b'+' { t - off } else { t + off } * SECOND);
    }
    parse_f3(s)
}

#[inline]
fn parse_f3(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    let mut sig = 0usize; // significant digits seen
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig > 19 {
                // keep validating the digits, but the value is out of range
                v = u64::MAX;
                continue;
            }
            v = v * 10 + d as u64;
        }
    }
    if sig > 19 || v > i64::MAX as u64 {
        // all digits, but larger than any i64: a valid form outside the window
        return Some(i64::MAX);
    }
    Some(v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn forms() {
        assert_eq!(parse(b"2026-10-03T23:58:10Z"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-03T19:58:10-04:00"), Some(1791071890000));
        assert_eq!(parse(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse(b"0001791071890000"), Some(1791071890000));
        assert_eq!(parse(b"2026-10-04T01:30:00+02:00"), parse(b"2026-10-03T23:30:00Z"));
        assert_eq!(parse(b"2026-10-03T23:58:10-00:00"), parse(b"2026-10-03T23:58:10Z"));
        for bad in [&b""[..], b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z", b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2027-02-29T00:00:00Z", b"2026-10-03T24:00:00Z",
            b"2026-10-03T23:60:00Z", b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00", b"2026-10-03T23:58:10+05:60",
            b"2026-10-03T23:58:10+0200", b"1791071890000.0", b"1.79e12", b"-5", b"2026-10-03T23:58:10.000Z"] {
            assert!(parse(bad).is_none() , "{:?}", std::str::from_utf8(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"179102160000000000000"), Some(i64::MAX));
    }
}
