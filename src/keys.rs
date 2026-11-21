//! IDs as 64-bit keys.
//!
//! An ID of 1 to 9 bytes, each ASCII 0x01..=0x7F, packs exactly into 63 bits:
//! seven bits per byte, first byte most significant, zero padding. The
//! packing keeps byte-wise order, so comparing two packed keys compares the
//! IDs. Every other non-empty ID is "weird": it is interned into a global
//! table and gets `WEIRD | index`. Weird keys are equal only to themselves;
//! ordering them needs `cmp_keys`.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering as AO};
use std::sync::{LazyLock, Mutex, RwLock};

pub const WEIRD: u64 = 1 << 63;

const LO: u64 = 0x0101_0101_0101_0101;
const HI: u64 = 0x8080_8080_8080_8080;

/// Pack a non-empty ID of at most 9 bytes in 0x01..=0x7F.
#[inline(always)]
pub fn pack(s: &[u8]) -> Option<u64> {
    let n = s.len();
    if n == 0 || n > 9 {
        return None;
    }
    let (w, b8) = if n >= 8 {
        (u64::from_le_bytes(s[..8].try_into().unwrap()), if n == 9 { s[8] as u64 } else { 0 })
    } else {
        let mut buf = [0u8; 8];
        buf[..n].copy_from_slice(s);
        (u64::from_le_bytes(buf), 0)
    };
    // every byte below 0x80, no zero byte inside the ID
    let fill = if n >= 8 { 0 } else { LO << (8 * n) };
    let z = w | fill;
    if (w | b8) & HI != 0 || (z.wrapping_sub(LO) & !z & HI) != 0 || (n == 9 && b8 == 0) {
        return None;
    }
    let mut x = w.swap_bytes();
    x = (x & 0x007F_007F_007F_007F) | ((x & 0x7F00_7F00_7F00_7F00) >> 1);
    x = (x & 0x0000_3FFF_0000_3FFF) | ((x & 0x3FFF_0000_3FFF_0000) >> 2);
    x = (x & 0x0000_0000_0FFF_FFFF) | ((x & 0x0FFF_FFFF_0000_0000) >> 4);
    Some((x << 7) | b8)
}

fn unpack(k: u64, out: &mut Vec<u8>) {
    let b8 = (k & 0x7F) as u8;
    let mut x = k >> 7;
    x = (x & 0x0FFF_FFFF) | ((x & 0x00FF_FFFF_F000_0000) << 4);
    x = (x & 0x0000_3FFF_0000_3FFF) | ((x & 0x0FFF_C000_0FFF_C000) << 2);
    x = (x & 0x007F_007F_007F_007F) | ((x & 0x3F80_3F80_3F80_3F80) << 1);
    for &b in x.to_be_bytes().iter().chain(std::iter::once(&b8)) {
        if b == 0 {
            break;
        }
        out.push(b);
    }
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
        unpack(k, out);
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
        let ids: Vec<&[u8]> = vec![b"0", b"00", b"1", b"A", b"Z", b"a", b"aa", b"ab", b"b", b"u1", b"u10", b"u2", b"zzzzzzzzz",
            b" u1", b"u1 ", b"A\tB", b"e3\r", b"\x01", b"\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f\x7f", b"abcdefgh", b"abcdefg", b"abcdefghi"];
        for a in &ids {
            for b in &ids {
                assert_eq!(pack(a).unwrap().cmp(&pack(b).unwrap()), a.cmp(b), "{:?} {:?}", a, b);
            }
            assert_eq!(&id_bytes(pack(a).unwrap())[..], *a);
        }
        for bad in [&b""[..], b"abcdefghij", b"a\x00b", b"\x80", b"abcdefgh\x00", b"\xc3\xa9"] {
            assert!(pack(bad).is_none(), "{:?}", bad);
        }
    }
}
