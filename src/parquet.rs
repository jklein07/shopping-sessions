//! A minimal Parquet writer: one row group, one uncompressed PLAIN data page
//! (v1) per column, flat schema of REQUIRED or OPTIONAL columns.

use std::fs::File;
use std::io::Write;

pub enum Values {
    I32(Vec<i32>),
    I64(Vec<i64>),
    /// Concatenated bytes and the end offset of each value.
    Str { bytes: Vec<u8>, ends: Vec<u32> },
}

pub enum Logical {
    None,
    String,
    Date,
}

pub struct Column {
    pub name: &'static str,
    pub logical: Logical,
    pub values: Values,
    /// For an OPTIONAL column: one flag per row (values hold only the present rows).
    pub present: Option<Vec<bool>>,
}

const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_STRUCT: u8 = 12;

struct Thrift {
    buf: Vec<u8>,
    last: Vec<i16>,
    cur: i16,
}

impl Thrift {
    fn new() -> Self {
        Thrift { buf: Vec::new(), last: Vec::new(), cur: 0 }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.buf.push(v as u8);
    }
    fn zigzag(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let delta = id - self.cur;
        if delta > 0 && delta <= 15 {
            self.buf.push(((delta as u8) << 4) | ty);
        } else {
            self.buf.push(ty);
            self.zigzag(id as i64);
        }
        self.cur = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, T_I32);
        self.zigzag(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, T_I64);
        self.zigzag(v);
    }
    fn binary(&mut self, id: i16, b: &[u8]) {
        self.field(id, T_BINARY);
        self.varint(b.len() as u64);
        self.buf.extend_from_slice(b);
    }
    fn begin(&mut self) {
        self.last.push(self.cur);
        self.cur = 0;
    }
    fn end(&mut self) {
        self.buf.push(0);
        self.cur = self.last.pop().unwrap();
    }
    fn struct_field(&mut self, id: i16) {
        self.field(id, T_STRUCT);
        self.begin();
    }
    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, T_LIST);
        if n < 15 {
            self.buf.push(((n as u8) << 4) | elem);
        } else {
            self.buf.push(0xF0 | elem);
            self.varint(n as u64);
        }
    }
}

fn physical(v: &Values) -> i32 {
    match v {
        Values::I32(_) => 1,
        Values::I64(_) => 2,
        Values::Str { .. } => 6,
    }
}

/// The little-endian bytes of a slice of integers (the in-memory layout on every target we build).
fn le_bytes<T>(x: &[T]) -> &[u8] {
    #[cfg(not(target_endian = "little"))]
    compile_error!("little-endian targets only");
    // SAFETY: plain integers, no padding; the byte view covers exactly the slice.
    unsafe { std::slice::from_raw_parts(x.as_ptr() as *const u8, std::mem::size_of_val(x)) }
}

fn encode_values(v: &Values, out: &mut Vec<u8>) {
    match v {
        Values::I32(x) => out.extend_from_slice(le_bytes(x)),
        Values::I64(x) => out.extend_from_slice(le_bytes(x)),
        Values::Str { bytes, ends } => {
            // each value: u32 length, then the bytes; short values copied 16 bytes at a time
            let total = bytes.len() + ends.len() * 4;
            out.reserve(total + 16);
            let base = out.len();
            let dst = out.as_mut_ptr();
            let mut w = base;
            let mut s = 0usize;
            for &e in ends {
                let e = e as usize;
                let n = e - s;
                // SAFETY: w + 4 + n <= base + total, and at most 16 bytes past that were reserved.
                unsafe {
                    std::ptr::copy_nonoverlapping((n as u32).to_le_bytes().as_ptr(), dst.add(w), 4);
                    if n <= 16 && s + 16 <= bytes.len() {
                        std::ptr::copy_nonoverlapping(bytes.as_ptr().add(s), dst.add(w + 4), 16);
                    } else {
                        std::ptr::copy_nonoverlapping(bytes.as_ptr().add(s), dst.add(w + 4), n);
                    }
                }
                w += 4 + n;
                s = e;
            }
            assert_eq!(w, base + total);
            // SAFETY: every byte in base..w was written above.
            unsafe { out.set_len(w) };
        }
    }
}

/// Write a file with one row group holding `rows` rows.
pub fn write(path: &std::path::Path, rows: usize, cols: &[Column]) -> std::io::Result<()> {
    // pages go straight to the file; only headers and the footer are built in memory
    let mut f = File::create(path)?;
    f.write_all(b"PAR1")?;
    let mut pos = 4i64;
    struct Meta {
        offset: i64,
        size: i64,
        nvalues: i64,
    }
    let mut metas = Vec::new();
    let mut page = Vec::new();
    for c in cols.iter().filter(|_| rows > 0) {
        page.clear();
        if let Some(present) = &c.present {
            // definition levels: RLE/bit-packed hybrid, one bit-packed run, with a u32 length prefix
            let groups = present.len().div_ceil(8);
            let mut rle = Vec::with_capacity(groups + 10);
            let mut h = ((groups as u64) << 1) | 1;
            while h >= 0x80 {
                rle.push((h as u8) | 0x80);
                h >>= 7;
            }
            rle.push(h as u8);
            for g in 0..groups {
                let mut b = 0u8;
                for k in 0..8 {
                    let i = g * 8 + k;
                    if i < present.len() && present[i] {
                        b |= 1 << k;
                    }
                }
                rle.push(b);
            }
            page.extend_from_slice(&(rle.len() as u32).to_le_bytes());
            page.extend_from_slice(&rle);
        }
        // a required integer column is written from the values themselves, without a copy
        let direct: Option<&[u8]> = match (&c.values, &c.present) {
            (Values::I32(x), None) => Some(le_bytes(x)),
            (Values::I64(x), None) => Some(le_bytes(x)),
            _ => None,
        };
        if direct.is_none() {
            encode_values(&c.values, &mut page);
        }
        let body: &[u8] = direct.unwrap_or(&page);
        let mut h = Thrift::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, body.len() as i32);
        h.i32(3, body.len() as i32);
        h.struct_field(5);
        h.i32(1, rows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3); // RLE
        h.end();
        h.buf.push(0);
        let offset = pos;
        f.write_all(&h.buf)?;
        f.write_all(body)?;
        pos += (h.buf.len() + body.len()) as i64;
        metas.push(Meta { offset, size: (h.buf.len() + body.len()) as i64, nvalues: rows as i64 });
    }
    // FileMetaData
    let mut t = Thrift::new();
    t.i32(1, 1);
    t.list(2, T_STRUCT, cols.len() + 1);
    t.begin();
    t.binary(4, b"schema");
    t.i32(5, cols.len() as i32);
    t.end();
    for c in cols {
        t.begin();
        t.i32(1, physical(&c.values));
        t.i32(3, if c.present.is_some() { 1 } else { 0 });
        t.binary(4, c.name.as_bytes());
        match c.logical {
            Logical::None => {}
            Logical::String => {
                t.i32(6, 0); // UTF8
                t.struct_field(10);
                t.struct_field(1);
                t.end();
                t.end();
            }
            Logical::Date => {
                t.i32(6, 6); // DATE
                t.struct_field(10);
                t.struct_field(6);
                t.end();
                t.end();
            }
        }
        t.end();
    }
    t.i64(3, rows as i64);
    if rows == 0 {
        t.list(4, T_STRUCT, 0);
        t.binary(6, b"shopping-sessions");
        t.buf.push(0);
        let n = t.buf.len() as u32;
        t.buf.extend_from_slice(&n.to_le_bytes());
        t.buf.extend_from_slice(b"PAR1");
        return f.write_all(&t.buf);
    }
    t.list(4, T_STRUCT, 1);
    t.begin();
    t.list(1, T_STRUCT, cols.len());
    let mut total = 0i64;
    for (c, m) in cols.iter().zip(&metas) {
        t.begin();
        t.i64(2, m.offset);
        t.struct_field(3);
        t.i32(1, physical(&c.values));
        t.list(2, T_I32, 2);
        t.zigzag(0);
        t.zigzag(3);
        t.list(3, T_BINARY, 1);
        t.varint(c.name.len() as u64);
        t.buf.extend_from_slice(c.name.as_bytes());
        t.i32(4, 0); // UNCOMPRESSED
        t.i64(5, m.nvalues);
        t.i64(6, m.size);
        t.i64(7, m.size);
        t.i64(9, m.offset);
        t.end();
        t.end();
        total += m.size;
    }
    t.i64(2, total);
    t.i64(3, rows as i64);
    t.end();
    t.binary(6, b"shopping-sessions");
    t.buf.push(0);
    let n = t.buf.len() as u32;
    t.buf.extend_from_slice(&n.to_le_bytes());
    t.buf.extend_from_slice(b"PAR1");
    f.write_all(&t.buf)?;
    Ok(())
}
