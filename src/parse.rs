//! CSV dialect, timestamps and prices, exactly as rules.md defines them.

use memchr::memchr;

pub type S = &'static [u8];

pub const SECOND: i64 = 1_000;
pub const DAY: i64 = 86_400_000;
pub const GAP: i64 = 1_800_000; // 30 minutes
pub const WEEK: i64 = 604_800_000; // 7 days

/// Split one line into exactly `out.len()` fields. Returns false when the line
/// is malformed: empty, wrong field count, or a quote anywhere except around
/// a whole field. Quotes are stripped; values are subslices of `line`.
#[inline]
pub fn split_line<'a>(line: &'a [u8], out: &mut [&'a [u8]]) -> bool {
    let len = line.len();
    if len == 0 {
        return false;
    }
    let n = out.len();
    let mut f = 0usize;
    let mut i = 0usize;
    loop {
        if f >= n {
            return false;
        }
        let end = match memchr(b',', &line[i..]) {
            Some(p) => i + p,
            None => len,
        };
        let raw = &line[i..end];
        let val = match memchr(b'"', raw) {
            None => raw,
            Some(q) => {
                let rl = raw.len();
                if q != 0 || rl < 2 || raw[rl - 1] != b'"' {
                    return false;
                }
                let inner = &raw[1..rl - 1];
                if memchr(b'"', inner).is_some() {
                    return false;
                }
                inner
            }
        };
        out[f] = val;
        f += 1;
        if end == len {
            break;
        }
        i = end + 1;
    }
    f == n
}

/// Split a header line into names (lenient: any count), stripping whole-field quotes.
pub fn header_names(line: &[u8]) -> Vec<&[u8]> {
    line.split(|&b| b == b',')
        .map(|f| {
            if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
                &f[1..f.len() - 1]
            } else {
                f
            }
        })
        .collect()
}

/// For each wanted column, its position in the file. Falls back to the
/// documented order if the header does not name exactly these columns.
pub fn column_map(header: &[u8], wanted: &[&str]) -> Vec<usize> {
    let names = header_names(header);
    let identity: Vec<usize> = (0..wanted.len()).collect();
    if names.len() != wanted.len() {
        return identity;
    }
    let mut map = Vec::with_capacity(wanted.len());
    for w in wanted {
        match names.iter().position(|n| *n == w.as_bytes()) {
            Some(p) => map.push(p),
            None => return identity,
        }
    }
    let mut seen = map.clone();
    seen.sort_unstable();
    seen.dedup();
    if seen.len() != wanted.len() {
        return identity;
    }
    map
}

#[inline]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a < 10 && c < 10 {
        Some((a as i64) * 10 + c as i64)
    } else {
        None
    }
}

#[inline]
fn is_leap(y: i64) -> bool {
    y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)
}

#[inline]
pub fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// `YYYY-MM-DDTHH:MM:SS` (first 19 bytes) to epoch ms, validating the calendar.
#[inline]
fn civil_ms(s: &[u8]) -> Option<i64> {
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let year = d2(s, 0)? * 100 + d2(s, 2)?;
    let month = d2(s, 5)?;
    let day = d2(s, 8)?;
    let hour = d2(s, 11)?;
    let minute = d2(s, 14)?;
    let second = d2(s, 17)?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 || day < 1 {
        return None;
    }
    let dim = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if is_leap(year) {
                29
            } else {
                28
            }
        }
    };
    if day > dim {
        return None;
    }
    let days = days_from_civil(year, month, day);
    Some(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1_000)
}

/// Remembers the last validated `YYYY-MM-DD` so repeated dates skip the
/// calendar math. Results are identical to `parse_ts`.
#[derive(Default)]
pub struct TsCache {
    date: [u8; 10],
    days: i64,
}

impl TsCache {
    #[inline]
    fn civil_ms(&mut self, s: &[u8]) -> Option<i64> {
        if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
            return None;
        }
        let days = if s[..10] == self.date {
            self.days
        } else {
            if s[4] != b'-' || s[7] != b'-' {
                return None;
            }
            let year = d2(s, 0)? * 100 + d2(s, 2)?;
            let month = d2(s, 5)?;
            let day = d2(s, 8)?;
            if !(1..=12).contains(&month) || day < 1 {
                return None;
            }
            let dim = match month {
                1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
                4 | 6 | 9 | 11 => 30,
                _ => {
                    if is_leap(year) {
                        29
                    } else {
                        28
                    }
                }
            };
            if day > dim {
                return None;
            }
            let d = days_from_civil(year, month, day);
            self.date.copy_from_slice(&s[..10]);
            self.days = d;
            d
        };
        let hour = d2(s, 11)?;
        let minute = d2(s, 14)?;
        let second = d2(s, 17)?;
        if hour > 23 || minute > 59 || second > 59 {
            return None;
        }
        Some(((days * 24 + hour) * 60 + minute) * 60_000 + second * 1_000)
    }

    /// Same contract as `parse_ts`.
    #[inline]
    pub fn parse(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 0 {
            return None;
        }
        if s[0].is_ascii_digit() && (n < 5 || s[4] != b'-') {
            return parse_ts(s);
        }
        if n == 20 {
            if s[19] != b'Z' {
                return None;
            }
            return self.civil_ms(s);
        }
        if n == 25 {
            let sign = match s[19] {
                b'+' => 1,
                b'-' => -1,
                _ => return None,
            };
            if s[22] != b':' {
                return None;
            }
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let local = self.civil_ms(s)?;
            return Some(local - sign * (oh * 60 + om) * 60_000);
        }
        None
    }
}

/// Parse F1, F2 or F3 into epoch ms. None means a bad timestamp (including an
/// F3 value too large for i64, which can never be inside a window).
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if s[0].is_ascii_digit() && (n < 5 || s[4] != b'-') {
        // F3: digits only.
        let mut v: u64 = 0;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v.checked_mul(10)?.checked_add(d as u64)?;
        }
        return if v > i64::MAX as u64 { None } else { Some(v as i64) };
    }
    if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        return civil_ms(s);
    }
    if n == 25 {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let local = civil_ms(s)?;
        return Some(local - sign * (oh * 60 + om) * 60_000);
    }
    None
}

/// Purchase price: ASCII digits only, value 1..=10,000,000.
#[inline]
pub fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            // keep scanning only to reject non-digits? value is already invalid
            return None;
        }
    }
    if v == 0 {
        None
    } else {
        Some(v as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps() {
        let f1 = parse_ts(b"2026-10-03T23:58:10Z").unwrap();
        assert_eq!(parse_ts(b"2026-10-03T19:58:10-04:00"), Some(f1));
        assert_eq!(parse_ts(b"1791071890000"), Some(1791071890000));
        assert_eq!(parse_ts(b"0001791071890000"), Some(1791071890000));
        assert_eq!(f1, 1791071890000);
        assert_eq!(parse_ts(b"2026-10-03T23:58:10-00:00"), Some(f1));
        for bad in [
            &b""[..], b"2026-10-03T23:58:10.000Z", b"2026-10-03T23:58:10z", b"2026-10-03t23:58:10Z",
            b"2026-10-03 23:58:10Z", b"2026-10-03T23:58:10", b"1791071890000.0", b"1.79e12", b"-5",
            b"2026-13-03T23:58:10Z", b"2026-02-30T23:58:10Z", b"2026-02-29T23:58:10Z",
            b"2026-10-03T24:00:00Z", b"2026-10-03T23:58:60Z", b"2026-10-03T23:58:10+24:00",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03T23:58:10+0200", b"99999999999999999999",
        ] {
            assert_eq!(parse_ts(bad), None, "{}", String::from_utf8_lossy(bad));
        }
        assert!(parse_ts(b"2028-02-29T00:00:00Z").is_some());
        assert_eq!(parse_ts(b"86400000"), Some(86_400_000));
        assert_eq!(parse_ts(b"000000000000000000000000001"), Some(1));
    }

    #[test]
    fn cache_agrees() {
        let mut c = TsCache::default();
        let cases: [&[u8]; 12] = [
            b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"2026-10-03T24:58:10Z",
            b"2026-02-29T00:00:00Z", b"2028-02-29T00:00:00Z", b"2028-02-29T00:00:60Z",
            b"2026-10-03T23:58:10+05:60", b"2026-10-03X23:58:10Z", b"0000000000000", b"2026-10-03T23:58:10z",
            b"2026/10/03T23:58:10Z", b"2026-10-03T23:58:10Z",
        ];
        for _ in 0..2 {
            for s in cases {
                assert_eq!(c.parse(s), parse_ts(s), "{}", String::from_utf8_lossy(s));
            }
        }
        // A cached date must not bypass separator checks on a different string.
        c.parse(b"2026-10-03T23:58:10Z");
        assert_eq!(c.parse(b"2026-10-03T23-58:10Z"), None);
    }

    #[test]
    fn prices() {
        assert_eq!(parse_price(b"0250"), Some(250));
        assert_eq!(parse_price(b"10000000"), Some(10_000_000));
        assert_eq!(parse_price(b"10000001"), None);
        assert_eq!(parse_price(b"0"), None);
        assert_eq!(parse_price(b"00"), None);
        assert_eq!(parse_price(b"19.99"), None);
        assert_eq!(parse_price(b"+5"), None);
        assert_eq!(parse_price(b""), None);
        assert_eq!(parse_price(b"1000\r"), None);
    }

    #[test]
    fn lines() {
        let mut f = [&b""[..]; 3];
        assert!(split_line(b"a,\"b\",", &mut f));
        assert_eq!(f, [&b"a"[..], b"b", b""]);
        assert!(split_line(b"\"\",x,y", &mut f));
        assert_eq!(f[0], b"");
        assert!(!split_line(b"a,b", &mut f));
        assert!(!split_line(b"a,b,c,d", &mut f));
        assert!(!split_line(b"\"a,b,c", &mut f));
        assert!(!split_line(b"a,b,c\"", &mut f));
        assert!(!split_line(b"a,b\"x\",c", &mut f));
        assert!(!split_line(b"a,\",c", &mut f));
        assert!(!split_line(b"", &mut f));
        assert!(split_line(b" a , b ,c ", &mut f));
        assert_eq!(f, [&b" a "[..], b" b ", b"c "]);
    }
}
