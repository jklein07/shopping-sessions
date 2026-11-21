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
    if ls >= e || ls >= len {
        return;
    }
    let mut fields: Fields<N> = [(0, 0); N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fs = ls; // current field start
    let mut qc_fs = 0u32; // quotes seen before fs (relative count)
    let mut qbase = 0u32; // quotes seen before the current block
    let mut blk = ls;
    let mut pad: [u8; 64];
    loop {
        let (comma, quote, nl) = if blk + 64 <= len {
            classify(data[blk..blk + 64].try_into().unwrap())
        } else {
            if blk >= len {
                return;
            }
            let rest = len - blk;
            pad = [0u8; 64];
            pad[..rest].copy_from_slice(&data[blk..]);
            if data[len - 1] != b'\n' {
                pad[rest] = b'\n'; // the unterminated last line ends at len
            }
            classify(&pad)
        };
        let mut m = comma | nl;
        while m != 0 {
            let off = m.trailing_zeros();
            let p = blk + off as usize;
            let qc = qbase + (quote & ((1u64 << off) - 1)).count_ones();
            let nq = qc - qc_fs;
            if nf < N {
                if nq == 0 {
                    fields[nf] = (fs, p);
                } else if nq == 2 && data[fs] == b'"' && data[p - 1] == b'"' {
                    fields[nf] = (fs + 1, p - 1);
                } else {
                    bad = true;
                }
            }
            nf += 1;
            if (nl >> off) & 1 != 0 {
                line(data, &fields, !bad && nf == N);
                nf = 0;
                bad = false;
                ls = p + 1;
                if ls >= e || ls >= len {
                    return;
                }
            }
            fs = p + 1;
            qc_fs = qc;
            m &= m - 1;
        }
        qbase += quote.count_ones();
        blk += 64;
        let _ = ls;
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
    #[test]
    fn chunks_and_tail() {
        // lines of varied length, some long, the last unterminated
        let mut data = b"h1,h2\n".to_vec();
        let mut want = Vec::new();
        for i in 0..500 {
            let a = "x".repeat(i % 97);
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
