//! The CSV dialect of rules.md: `\n` ends a line, `,` separates fields, a
//! field may be wrapped in double quotes (syntax only), a quote anywhere else
//! or a wrong field count makes the line malformed. An empty field is null.

/// 1 for the bytes that end an unquoted run: `"`, `,`, `\n`.
static STOP: [u8; 256] = {
    let mut t = [0u8; 256];
    t[b'"' as usize] = 1;
    t[b',' as usize] = 1;
    t[b'\n' as usize] = 1;
    t
};

/// Field bounds within the buffer, quotes stripped.
pub type Fields<const N: usize> = [(usize, usize); N];

/// Split the line that starts at `p`. The buffer must hold a `\n` at or after
/// `p`. Returns the start of the next line and whether the line is well formed
/// with exactly N fields.
#[inline(always)]
pub fn split<const N: usize>(buf: &[u8], mut p: usize, f: &mut Fields<N>) -> (usize, bool) {
    let ptr = buf.as_ptr();
    // SAFETY: the caller guarantees a '\n' at or after p inside buf, and every
    // loop below stops at '\n'.
    let at = |i: usize| unsafe { *ptr.add(i) };
    let mut i = 0usize;
    loop {
        let (s, e);
        if at(p) == b'"' {
            s = p + 1;
            let mut q = s;
            while STOP[at(q) as usize] == 0 {
                q += 1;
            }
            if at(q) != b'"' {
                return (skip_line(buf, q), false);
            }
            e = q;
            p = q + 1;
            let c = at(p);
            if c != b',' && c != b'\n' {
                return (skip_line(buf, p), false);
            }
        } else {
            s = p;
            let mut q = p;
            while STOP[at(q) as usize] == 0 {
                q += 1;
            }
            if at(q) == b'"' {
                return (skip_line(buf, q), false);
            }
            e = q;
            p = q;
        }
        if i < N {
            f[i] = (s, e);
        }
        i += 1;
        if at(p) == b'\n' {
            return (p + 1, i == N);
        }
        p += 1; // the comma
    }
}

#[inline]
fn skip_line(buf: &[u8], from: usize) -> usize {
    let mut q = from;
    while buf[q] != b'\n' {
        q += 1;
    }
    q + 1
}

/// Calls `line(buf, start)` for every data line that starts in [s, e) of a
/// file, skipping the header (the file's first line). A last line without
/// `\n` is copied into a terminated buffer first.
pub fn for_lines(data: &[u8], s: usize, e: usize, mut line: impl FnMut(&[u8], usize) -> usize) {
    let len = data.len();
    if len == 0 {
        return;
    }
    let mut p = s;
    if p == 0 {
        // header
        match memchr_nl(data, 0) {
            Some(q) => p = q + 1,
            None => return,
        }
    } else if data[p - 1] != b'\n' {
        match memchr_nl(data, p) {
            Some(q) => p = q + 1,
            None => return,
        }
    }
    let last_nl = match data.iter().rposition(|&c| c == b'\n') {
        Some(q) => q,
        None => return,
    };
    let stop = e.min(last_nl + 1);
    while p < stop {
        p = line(data, p);
    }
    if p < e && p > last_nl && p < len {
        // unterminated last line
        let mut tail = data[p..].to_vec();
        tail.push(b'\n');
        line(&tail, 0);
    }
}

#[inline]
pub fn memchr_nl(data: &[u8], from: usize) -> Option<usize> {
    data[from..].iter().position(|&c| c == b'\n').map(|i| i + from)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sp<const N: usize>(s: &[u8]) -> Option<Vec<Vec<u8>>> {
        let mut buf = s.to_vec();
        buf.push(b'\n');
        let mut f = [(0, 0); N];
        let (next, ok) = split::<N>(&buf, 0, &mut f);
        assert_eq!(next, buf.len());
        ok.then(|| f.iter().map(|&(a, b)| buf[a..b].to_vec()).collect())
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
        assert_eq!(sp::<2>(b" a ,\" b\""), Some(vec![b" a ".to_vec(), b" b".to_vec()]));
        assert_eq!(sp::<2>(b"a,b\r"), Some(vec![b"a".to_vec(), b"b\r".to_vec()]));
        assert_eq!(sp::<2>(b","), Some(vec![b"".to_vec(), b"".to_vec()]));
    }
}
