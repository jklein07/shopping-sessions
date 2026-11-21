//! The CSV dialect of rules.md: `\n` ends a line, `,` separates fields, a
//! field may be wrapped in double quotes (syntax only), a quote anywhere else
//! or a wrong field count makes the line malformed. An empty field is null.
//!
//! `scan` finds structure 64 bytes at a time with SIMD compares: bit masks of
//! commas, quotes and newlines. A field between two separators is well formed
//! when it holds no quote, or exactly two that are its first and last byte.

/// Field bounds within the buffer, quotes stripped.
pub type Fields<const N: usize> = [(usize, usize); N];

#[inline(always)]
fn classify(block: &[u8; 64]) -> (u64, u64, u64) {
    imp::classify(block.as_ptr())
}

#[cfg(target_arch = "x86_64")]
mod imp {
    #[inline(always)]
    pub fn classify(p: *const u8) -> (u64, u64, u64) {
        use std::arch::x86_64::*;
        // SAFETY: p points at 64 readable bytes.
        unsafe {
            #[cfg(target_feature = "avx2")]
            {
                let a = _mm256_loadu_si256(p as *const __m256i);
                let b = _mm256_loadu_si256(p.add(32) as *const __m256i);
                let m = |c: u8| {
                    let cv = _mm256_set1_epi8(c as i8);
                    let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, cv)) as u32 as u64;
                    let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, cv)) as u32 as u64;
                    lo | (hi << 32)
                };
                (m(b','), m(b'"'), m(b'\n'))
            }
            #[cfg(not(target_feature = "avx2"))]
            {
                let v = [
                    _mm_loadu_si128(p as *const __m128i),
                    _mm_loadu_si128(p.add(16) as *const __m128i),
                    _mm_loadu_si128(p.add(32) as *const __m128i),
                    _mm_loadu_si128(p.add(48) as *const __m128i),
                ];
                let m = |c: u8| {
                    let cv = _mm_set1_epi8(c as i8);
                    let mut r = 0u64;
                    for (i, x) in v.iter().enumerate() {
                        r |= (_mm_movemask_epi8(_mm_cmpeq_epi8(*x, cv)) as u16 as u64) << (16 * i);
                    }
                    r
                };
                (m(b','), m(b'"'), m(b'\n'))
            }
        }
    }
}

#[cfg(target_arch = "aarch64")]
mod imp {
    #[inline(always)]
    pub fn classify(p: *const u8) -> (u64, u64, u64) {
        use std::arch::aarch64::*;
        // SAFETY: p points at 64 readable bytes.
        unsafe {
            let v0 = vld1q_u8(p);
            let v1 = vld1q_u8(p.add(16));
            let v2 = vld1q_u8(p.add(32));
            let v3 = vld1q_u8(p.add(48));
            const BITS: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
            let bits = vld1q_u8(BITS.as_ptr());
            let m = |c: u8| {
                let cv = vdupq_n_u8(c);
                let t0 = vandq_u8(vceqq_u8(v0, cv), bits);
                let t1 = vandq_u8(vceqq_u8(v1, cv), bits);
                let t2 = vandq_u8(vceqq_u8(v2, cv), bits);
                let t3 = vandq_u8(vceqq_u8(v3, cv), bits);
                let s0 = vpaddq_u8(t0, t1);
                let s1 = vpaddq_u8(t2, t3);
                let s = vpaddq_u8(s0, s1);
                let s = vpaddq_u8(s, s);
                vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
            };
            (m(b','), m(b'"'), m(b'\n'))
        }
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
mod imp {
    #[inline(always)]
    pub fn classify(p: *const u8) -> (u64, u64, u64) {
        let (mut c, mut q, mut n) = (0u64, 0u64, 0u64);
        for i in 0..64 {
            let b = unsafe { *p.add(i) };
            c |= ((b == b',') as u64) << i;
            q |= ((b == b'"') as u64) << i;
            n |= ((b == b'\n') as u64) << i;
        }
        (c, q, n)
    }
}

/// Position of the first data line at or after `s` (lines belong to the chunk
/// holding their first byte; the file's first line is the header).
fn first_line(data: &[u8], s: usize) -> Option<usize> {
    if s == 0 || data[s - 1] != b'\n' {
        data[s..].iter().position(|&c| c == b'\n').map(|i| s + i + 1)
    } else {
        Some(s)
    }
}

/// Split one line that starts at `ls` using a 128-byte window (needs
/// `ls + 128 <= data.len()`). None if the line is longer than the window.
#[inline(always)]
fn line_fast<const N: usize>(data: &[u8], ls: usize, f: &mut Fields<N>) -> Option<(usize, bool)> {
    let (c0, q0, n0) = classify(data[ls..ls + 64].try_into().unwrap());
    let (c1, q1, n1) = classify(data[ls + 64..ls + 128].try_into().unwrap());
    let nl = n0 as u128 | (n1 as u128) << 64;
    if nl == 0 {
        return None;
    }
    let end = nl.trailing_zeros();
    let below = (1u128 << end) - 1;
    let commas = (c0 as u128 | (c1 as u128) << 64) & below;
    let next = ls + end as usize + 1;
    if commas.count_ones() as usize != N - 1 {
        return Some((next, false));
    }
    let q = (q0 as u128 | (q1 as u128) << 64) & below;
    let mut sep = commas | (1u128 << end);
    if q == 0 {
        let mut start = 0u32;
        for fi in f.iter_mut() {
            let p = sep.trailing_zeros();
            sep &= sep - 1;
            *fi = (ls + start as usize, ls + p as usize);
            start = p + 1;
        }
        return Some((next, true));
    }
    // Quoted fields, without a per-field loop over quote ranges. A line is well formed when
    // (a) every quote is the first or the last byte of its field, and (b) every field holds an
    // even number of quotes (the quote parity is even at each separator). Then each field has
    // either no quote, or exactly two: its first and last byte (a 1-byte field cannot hold two).
    let upto = below | (1u128 << end); // positions 0..=end
    let starts = ((sep << 1) | 1) & upto; // first byte of each field (= its separator if empty)
    let ends = sep >> 1; // last byte of each non-empty field (and some separators, harmless)
    let (ql, qh) = (q as u64, (q >> 64) as u64);
    let pl = prefix_xor(ql);
    let ph = prefix_xor(qh) ^ 0u64.wrapping_sub(pl >> 63);
    let parity = pl as u128 | (ph as u128) << 64;
    let ok = q & !(starts | ends) == 0 && parity & sep == 0;
    let open = q & starts;
    let close = q & ends;
    let mut a = (starts & !open) | (open << 1);
    let mut b = (sep & !(close << 1)) | close;
    for fi in f.iter_mut() {
        let x = a.trailing_zeros();
        let y = b.trailing_zeros();
        a &= a.wrapping_sub(1);
        b &= b.wrapping_sub(1);
        *fi = (ls + x as usize, ls + y as usize);
    }
    Some((next, ok))
}

/// Bit i of the result: XOR of bits 0..=i of x.
#[inline(always)]
fn prefix_xor(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

/// The general path: any line length, and a last line without `\n`.
fn line_slow<const N: usize>(data: &[u8], ls: usize, f: &mut Fields<N>) -> (usize, bool) {
    let len = data.len();
    let end = data[ls..].iter().position(|&c| c == b'\n').map_or(len, |i| ls + i);
    let mut nf = 0usize;
    let mut bad = false;
    let mut fs = ls;
    for p in ls..=end {
        if p == end || data[p] == b',' {
            let field = &data[fs..p];
            let nq = field.iter().filter(|&&c| c == b'"').count();
            let (a, b) = if nq == 0 {
                (fs, p)
            } else if nq == 2 && field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
                (fs + 1, p - 1)
            } else {
                bad = true;
                (fs, p)
            };
            if nf < N {
                f[nf] = (a, b);
            }
            nf += 1;
            fs = p + 1;
        }
    }
    (end + 1, !bad && nf == N)
}

/// Calls `line(data, fields, ok)` for every data line that starts in [s, e)
/// of a file. `ok` is false for a malformed line (fields are then unspecified).
/// A final line without `\n` is still a line.
#[inline(always)]
pub fn scan<const N: usize>(data: &[u8], s: usize, e: usize, mut line: impl FnMut(&[u8], &Fields<N>, bool)) {
    let len = data.len();
    let mut ls = match first_line(data, s) {
        Some(p) => p,
        None => return,
    };
    let mut fields: Fields<N> = [(0, 0); N];
    let stop = e.min(len);
    while ls < stop {
        if ls + 128 <= len {
            if let Some((next, ok)) = line_fast(data, ls, &mut fields) {
                line(data, &fields, ok);
                ls = next;
                continue;
            }
        }
        let (next, ok) = line_slow(data, ls, &mut fields);
        line(data, &fields, ok);
        ls = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sp<const N: usize>(s: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut buf = b"header\n".to_vec();
        buf.extend_from_slice(s);
        buf.push(b'\n');
        let mut out = None;
        let mut calls = 0;
        scan::<N>(&buf, 0, buf.len(), |d, f, ok| {
            calls += 1;
            out = ok.then(|| f.iter().map(|&(a, b)| d[a..b].to_vec()).collect());
        });
        assert_eq!(calls, 1);
        out
    }
    #[test]
    fn dialect() {
        assert_eq!(sp::<2>(b"a,b"), Some(vec![b"a".to_vec(), b"b".to_vec()]));
        assert_eq!(sp::<2>(b"\"a\",\"\""), Some(vec![b"a".to_vec(), b"".to_vec()]));
        assert_eq!(sp::<2>(b"a,b,c"), None);
        assert_eq!(sp::<2>(b"a"), None);
        assert_eq!(sp::<2>(b""), None);
        assert_eq!(sp::<2>(b"a,b\""), None);
        assert_eq!(sp::<2>(b"\"a,b"), None);
        assert_eq!(sp::<2>(b"a\"x\",b"), None);
        assert_eq!(sp::<2>(b"\"a\"x,b"), None);
        assert_eq!(sp::<2>(b"\",b"), None);
        assert_eq!(sp::<2>(b"\"\"\",b"), None);
        assert_eq!(sp::<2>(b" a ,\" b\""), Some(vec![b" a ".to_vec(), b" b".to_vec()]));
        assert_eq!(sp::<2>(b"a,b\r"), Some(vec![b"a".to_vec(), b"b\r".to_vec()]));
        assert_eq!(sp::<2>(b","), Some(vec![b"".to_vec(), b"".to_vec()]));
    }
    fn diff<const N: usize>(iters: usize, seed: u64) {
        let mut st = seed;
        let mut rnd = move || {
            st ^= st << 13;
            st ^= st >> 7;
            st ^= st << 17;
            st
        };
        let alpha = b"ab,,\"\"\" \r";
        let mut fa: Fields<N> = [(0, 0); N];
        let mut valid = 0usize;
        let mut fb: Fields<N> = [(0, 0); N];
        for it in 0..iters {
            let mut buf: Vec<u8> = if it % 2 == 0 {
                let len = (rnd() % 140) as usize;
                (0..len).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect()
            } else {
                // field by field: mostly the right count, plain or quoted, sometimes corrupted
                let nf = match rnd() % 8 {
                    0 => N.saturating_sub(1).max(1),
                    1 => N + 1,
                    _ => N,
                };
                let mut v = Vec::new();
                for k in 0..nf {
                    if k > 0 {
                        v.push(b',');
                    }
                    let flen = (rnd() % 12) as usize;
                    let body: Vec<u8> = (0..flen).map(|_| b"ab \r"[(rnd() % 4) as usize]).collect();
                    match rnd() % (6 * N as u64) {
                        0 => {
                            let mut b2 = body.clone();
                            let at = (rnd() as usize) % (b2.len() + 1);
                            b2.insert(at, b'"');
                            if rnd() % 2 == 0 {
                                b2.insert(0, b'"');
                            }
                            v.extend_from_slice(&b2);
                        }
                        x if x % 2 == 0 => v.extend_from_slice(&body),
                        _ => {
                            v.push(b'"');
                            v.extend_from_slice(&body);
                            v.push(b'"');
                        }
                    }
                }
                v
            };
            let len = buf.len();
            buf.push(b'\n');
            buf.extend(std::iter::repeat(b'x').take(130));
            if let Some((na, oka)) = line_fast::<N>(&buf, 0, &mut fa) {
                let (nb, okb) = line_slow::<N>(&buf, 0, &mut fb);
                assert_eq!((na, oka), (nb, okb), "{:?}", String::from_utf8_lossy(&buf[..len]));
                if oka {
                    valid += 1;
                    assert_eq!(fa, fb, "{:?}", String::from_utf8_lossy(&buf[..len]));
                }
            } else {
                assert!(len >= 128);
            }
        }
        assert!(valid > iters / 20, "only {valid} well-formed lines");
    }
    #[test]
    fn fast_matches_slow() {
        diff::<1>(300_000, 1);
        diff::<2>(1_000_000, 2);
        diff::<4>(1_000_000, 3);
        diff::<7>(1_000_000, 4);
        diff::<12>(300_000, 5);
    }
    #[test]
    fn chunks_and_tail() {
        // lines of varied length, some long, the last unterminated
        let mut data = b"h1,h2\n".to_vec();
        let mut want = Vec::new();
        for i in 0..500 {
            let a = "x".repeat(i % 197);
            let line = format!("{a}{i},\"q{i}\"");
            want.push((format!("{a}{i}"), format!("q{i}")));
            data.extend_from_slice(line.as_bytes());
            if i != 499 {
                data.push(b'\n');
            }
        }
        for chunk in [1usize, 7, 64, 100, 4096, 1 << 20] {
            let mut got = Vec::new();
            let mut s = 0;
            while s < data.len() {
                let e = (s + chunk).min(data.len());
                scan::<2>(&data, s, e, |d, f, ok| {
                    assert!(ok);
                    got.push((String::from_utf8(d[f[0].0..f[0].1].to_vec()).unwrap(), String::from_utf8(d[f[1].0..f[1].1].to_vec()).unwrap()));
                });
                s = e;
            }
            assert_eq!(got, want, "chunk {chunk}");
        }
    }
}
