//! Timestamps: F1 `YYYY-MM-DDTHH:MM:SSZ`, F2 `YYYY-MM-DDTHH:MM:SS+HH:MM`,
//! F3 epoch milliseconds (ASCII digits only). Anything else is bad (None).
//! Parsing is SWAR: 8 bytes at a time in a u64.

pub const SECOND: i64 = 1000;
pub const MINUTE: i64 = 60 * SECOND;
pub const HOUR: i64 = 60 * MINUTE;
pub const DAY: i64 = 24 * HOUR;

const ZEROS: u64 = 0x3030_3030_3030_3030;

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
fn dim(y: i64, m: i64) -> i64 {
    const D: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if m == 2 && y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
        29
    } else {
        D[m as usize]
    }
}

/// First day of each month, 1970-01 .. 2100-01, as days since the epoch.
const Y0: i64 = 1970;
const YEARS: usize = 130;
static MONTH_START: [i32; YEARS * 12 + 1] = {
    let mut t = [0i32; YEARS * 12 + 1];
    let mut i = 0;
    while i <= YEARS * 12 {
        let y = Y0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        // days_from_civil(y, m, 1), written const-friendly
        let yy = if m <= 2 { y - 1 } else { y };
        let era = yy / 400;
        let yoe = yy - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        t[i] = (era * 146_097 + doe - 719_468) as i32;
        i += 1;
    }
    t
};

/// Days since the epoch of a valid-or-not (y, mo, d), or None if impossible.
#[inline(always)]
fn civil_days(y: i64, mo: i64, d: i64) -> Option<i64> {
    if mo < 1 || mo > 12 || d < 1 {
        return None;
    }
    let i = (y - Y0) * 12 + mo - 1;
    if i >= 0 && (i as usize) < YEARS * 12 {
        let a = MONTH_START[i as usize] as i64;
        let b = MONTH_START[i as usize + 1] as i64;
        if d > b - a {
            return None;
        }
        return Some(a + d - 1);
    }
    if d > dim(y, mo) {
        return None;
    }
    Some(days_from_civil(y, mo, d))
}

/// Every byte of x is an ASCII digit.
#[inline(always)]
fn all_digits(x: u64) -> bool {
    const HI: u64 = 0xF0F0_F0F0_F0F0_F0F0;
    (x & HI) == ZEROS && (x.wrapping_add(0x0606_0606_0606_0606) & HI) == ZEROS
}

#[inline(always)]
fn le8(s: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(s[at..at + 8].try_into().unwrap())
}

/// Two-digit groups: byte i of the result is 10*d[i] + d[i+1].
#[inline(always)]
fn pairs(x: u64) -> u64 {
    let t = x.wrapping_sub(ZEROS);
    t.wrapping_mul(10).wrapping_add(t >> 8)
}

/// Seconds since the epoch of the `YYYY-MM-DDTHH:MM:SS` prefix (needs 20 bytes).
#[inline(always)]
fn civil_prefix(s: &[u8]) -> Option<i64> {
    let a = le8(s, 0); // YYYY-MM-
    let b = le8(s, 8); // DDTHH:MM
    let c = u32::from_le_bytes(s[16..20].try_into().unwrap()) as u64; // :SS?
    const A_SEP: u64 = 0xFF00_00FF_0000_0000;
    const A_V: u64 = ((b'-' as u64) << 32) | ((b'-' as u64) << 56);
    const B_SEP: u64 = 0x0000_FF00_00FF_0000;
    const B_V: u64 = ((b'T' as u64) << 16) | ((b':' as u64) << 40);
    if a & A_SEP != A_V || b & B_SEP != B_V || c & 0xFF != b':' as u64 {
        return None;
    }
    let a = (a & !A_SEP) | (ZEROS & A_SEP);
    let b = (b & !B_SEP) | (ZEROS & B_SEP);
    let c = (c & 0x00FF_FF00) | 0x3030_3030_3000_0030;
    if !all_digits(a) || !all_digits(b) || !all_digits(c) {
        return None;
    }
    let ua = pairs(a);
    let ub = pairs(b);
    let uc = pairs(c);
    let y = ((ua & 0xFF) * 100 + ((ua >> 16) & 0xFF)) as i64;
    let mo = ((ua >> 40) & 0xFF) as i64;
    let d = (ub & 0xFF) as i64;
    let h = ((ub >> 24) & 0xFF) as i64;
    let mi = ((ub >> 48) & 0xFF) as i64;
    let se = ((uc >> 8) & 0xFF) as i64;
    if h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let days = civil_days(y, mo, d)?;
    Some(((days * 24 + h) * 60 + mi) * 60 + se)
}

/// Milliseconds since the epoch, or None for a bad timestamp. An all-digit F3
/// value too large for i64 comes back as i64::MAX (outside every window).
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
        let o = u32::from_le_bytes(s[21..25].try_into().unwrap()) as u64; // H:MM, s[20] separately
        let oh1 = s[20].wrapping_sub(b'0');
        let oh2 = (o & 0xFF) as u8;
        let om1 = ((o >> 16) & 0xFF) as u8;
        let om2 = ((o >> 24) & 0xFF) as u8;
        let (oh2, om1, om2) = (oh2.wrapping_sub(b'0'), om1.wrapping_sub(b'0'), om2.wrapping_sub(b'0'));
        if oh1 > 9 || oh2 > 9 || om1 > 9 || om2 > 9 {
            return None;
        }
        let oh = (oh1 * 10 + oh2) as i64;
        let om = (om1 * 10 + om2) as i64;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * 60;
        return Some(if s[19] == b'+' { t - off } else { t + off } * SECOND);
    }
    parse_f3(s)
}

#[inline(always)]
fn eight(x: u64) -> u64 {
    // x: 8 ASCII digits, first (most significant) in the lowest byte
    let v = pairs(x);
    let v = ((v & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((v >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32;
    v
}

#[inline]
fn parse_f3(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if (9..=16).contains(&n) {
        // two overlapping 8-digit loads: digits 0..8 and n-8..n
        let hi = le8(s, 0);
        let lo = le8(s, n - 8);
        if !all_digits(hi) || !all_digits(lo) {
            return None;
        }
        const P10: [u64; 9] = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000];
        let k = P10[n - 8];
        return Some((eight(hi) * k + eight(lo) % k) as i64);
    }
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        if sig > 0 || d != 0 {
            sig += 1;
            if sig <= 19 {
                v = v * 10 + d as u64;
            }
        }
    }
    if sig > 19 || v > i64::MAX as u64 {
        return Some(i64::MAX);
    }
    Some(v as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slow(s: &[u8]) -> Option<i64> {
        // straightforward reference parser
        let n = s.len();
        if n > 0 && s.iter().all(|c| c.is_ascii_digit()) {
            let t = s.iter().skip_while(|&&c| c == b'0').count();
            if t > 19 {
                return Some(i64::MAX);
            }
            let v: u128 = std::str::from_utf8(s).unwrap().parse().unwrap();
            return Some(if v > i64::MAX as u128 { i64::MAX } else { v as i64 });
        }
        if n != 20 && n != 25 {
            return None;
        }
        let num = |a: usize, b: usize| -> Option<i64> {
            let p = &s[a..b];
            p.iter().all(|c| c.is_ascii_digit()).then(|| std::str::from_utf8(p).unwrap().parse().unwrap())
        };
        if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let (y, mo, d, h, mi, se) = (num(0, 4)?, num(5, 7)?, num(8, 10)?, num(11, 13)?, num(14, 16)?, num(17, 19)?);
        if !(1..=12).contains(&mo) || d < 1 || d > dim(y, mo) || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let t = (((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60 + se) * SECOND;
        if n == 20 {
            return (s[19] == b'Z').then_some(t);
        }
        if (s[19] != b'+' && s[19] != b'-') || s[22] != b':' {
            return None;
        }
        let (oh, om) = (num(20, 22)?, num(23, 25)?);
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * MINUTE;
        Some(if s[19] == b'+' { t - off } else { t + off })
    }

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
            b"2026-10-03T23:58:10+0200", b"1791071890000.0", b"1.79e12", b"-5", b"2026-10-03T23:58:10.000Z",
            b"2026-00-03T23:58:10Z", b"2026-10-00T23:58:10Z", b"\xef\xbc\x92026-10-03T23:58:10Z"] {
            assert!(parse(bad).is_none() , "{:?}", String::from_utf8_lossy(bad));
        }
        assert!(parse(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse(b"179102160000000000000"), Some(i64::MAX));
        assert_eq!(parse(b"86400000"), Some(86400000));
        assert_eq!(parse(b"0"), Some(0));
    }

    #[test]
    fn random_against_slow() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut next = || {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x
        };
        let alphabet = b"0123456789-T:Z+ z.e";
        for _ in 0..2_000_000 {
            let r = next();
            let mut s: Vec<u8> = match r % 5 {
                4 => format!("{:0w$}", (r >> 8) % 100_000_000_000_000_000, w = ((r >> 3) % 20) as usize).into_bytes(),
                0 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", r % 10000, (r >> 14) % 14, (r >> 18) % 33, (r >> 24) % 26, (r >> 30) % 62, (r >> 36) % 62).into_bytes(),
                1 => format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", 2000 + r % 40, 1 + (r >> 14) % 12, 1 + (r >> 18) % 31, (r >> 24) % 24, (r >> 30) % 60, (r >> 36) % 60, if r >> 63 == 1 { '+' } else { '-' }, (r >> 42) % 26, (r >> 48) % 62).into_bytes(),
                2 => ((r >> 3) % 10_000_000_000_000_000).to_string().into_bytes(),
                _ => (0..(r % 27)).map(|i| alphabet[((r >> (i % 50)) as usize + i as usize * 7) % alphabet.len()]).collect(),
            };
            if next() % 5 == 0 && !s.is_empty() {
                let i = (next() as usize) % s.len();
                s[i] = alphabet[(next() as usize) % alphabet.len()];
            }
            assert_eq!(parse(&s), slow(&s), "{:?}", String::from_utf8_lossy(&s));
        }
    }
}
