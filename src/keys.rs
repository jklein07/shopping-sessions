//! IDs as 64-bit keys.
//!
//! An ID of 1 to 10 ASCII letters and digits packs exactly into a base-63
//! number (digit 0 is padding), and the packing keeps byte-wise order, so
//! comparing two packed keys compares the IDs. Every other non-empty ID is
//! "weird": it is interned into a global table and gets `WEIRD | index`.
//! Weird keys are equal only to themselves; ordering them needs `cmp_keys`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AO};
use std::sync::{LazyLock, Mutex, RwLock};

pub const WEIRD: u64 = 1 << 63;

const fn alnum_table() -> [u8; 256] {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 10 {
        t[b'0' as usize + i] = 1 + i as u8;
        i += 1;
    }
    let mut i = 0;
    while i < 26 {
        t[b'A' as usize + i] = 11 + i as u8;
        t[b'a' as usize + i] = 37 + i as u8;
        i += 1;
    }
    t
}
pub static ALNUM: [u8; 256] = alnum_table();
const DIGIT_CHAR: &[u8; 63] = b"\x000123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

const fn pow63() -> [u64; 10] {
    let mut p = [0u64; 10];
    let mut i = 0;
    while i < 10 {
        let mut v = 1u64;
        let mut j = 0;
        while j < 9 - i {
            v *= 63;
            j += 1;
        }
        p[i] = v;
        i += 1;
    }
    p
}
pub static POW63: [u64; 10] = pow63();

/// Pack a non-empty ID of at most 10 ASCII alphanumerics.
#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 10 {
        return None;
    }
    let mut v = 0u64;
    for i in 0..n {
        let d = ALNUM[s[i] as usize];
        if d == 0 {
            return None;
        }
        v += d as u64 * POW63[i];
    }
    Some(v)
}

struct Interner {
    map: HashMap<Vec<u8>, u64>,
}
static INTERN: LazyLock<Mutex<Interner>> = LazyLock::new(|| Mutex::new(Interner { map: HashMap::new() }));
static WEIRD_STRINGS: RwLock<Vec<Vec<u8>>> = RwLock::new(Vec::new());
pub static ANY_WEIRD: AtomicBool = AtomicBool::new(false);

#[cold]
fn intern(s: &[u8]) -> u64 {
    let mut g = INTERN.lock().unwrap();
    if let Some(&k) = g.map.get(s) {
        return k;
    }
    let mut strings = WEIRD_STRINGS.write().unwrap();
    let k = WEIRD | strings.len() as u64;
    strings.push(s.to_vec());
    g.map.insert(s.to_vec(), k);
    ANY_WEIRD.store(true, AO::Relaxed);
    k
}

/// The key of a non-empty ID.
#[inline(always)]
pub fn key(s: &[u8]) -> u64 {
    match pack(s) {
        Some(k) => k,
        None => intern(s),
    }
}

/// Append the ID a key stands for.
pub fn write_id(k: u64, out: &mut Vec<u8>) {
    if k & WEIRD != 0 {
        let strings = WEIRD_STRINGS.read().unwrap();
        out.extend_from_slice(&strings[(k & !WEIRD) as usize]);
    } else {
        for i in 0..10 {
            let d = (k / POW63[i]) % 63;
            if d == 0 {
                break;
            }
            out.push(DIGIT_CHAR[d as usize]);
        }
    }
}

pub fn id_bytes(k: u64) -> Vec<u8> {
    let mut v = Vec::new();
    write_id(k, &mut v);
    v
}

/// Byte-wise order of the IDs two keys stand for.
#[inline]
pub fn cmp_keys(a: u64, b: u64) -> Ordering {
    if (a | b) & WEIRD == 0 {
        a.cmp(&b)
    } else {
        id_bytes(a).cmp(&id_bytes(b))
    }
}

#[inline(always)]
pub fn hash(k: u64) -> u64 {
    let h = k.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    h ^ (h >> 29)
}

#[inline(always)]
pub fn hash2(k: u64) -> u64 {
    let h = (k ^ 0x5bd1_e995_1234_5679).wrapping_mul(0xD6E8_FEB8_6659_FD93);
    h ^ (h >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn order() {
        let ids: Vec<&[u8]> = vec![b"0", b"00", b"1", b"A", b"Z", b"a", b"aa", b"ab", b"b", b"u1", b"u10", b"u2", b"zzzzzzzzzz"];
        for a in &ids {
            for b in &ids {
                assert_eq!(pack(a).unwrap().cmp(&pack(b).unwrap()), a.cmp(b), "{:?} {:?}", a, b);
            }
            assert_eq!(&id_bytes(pack(a).unwrap())[..], *a);
        }
    }
}
