//! Shopping Sessions: messy click-stream CSV in /data, two daily Parquet
//! tables out. See rules.md for the rules and README.md for the design.
//!
//! Phases (each parallel over all cores):
//!   A. links: parse, concurrent hash table of IDs + lock-free union-find,
//!      each person named by the byte-wise smallest ID.
//!   B. parse events (radix-partitioned by event_id) and clicks
//!      (validated, partitioned by person).
//!   C. per event_id partition: keep the copy with the smallest ingest_ts,
//!      then repartition the surviving events by person.
//!   D. per person partition: group, sort by ts, sessionize, credit and
//!      assists, aggregate the two tables.
//!   E. write Parquet.

mod csv;
mod keys;
mod parquet;
mod ts;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering::*};
use std::time::Instant;

use ts::DAY;

const GAP: u32 = 30 * 60 * 1000;
const REACH: i64 = 7 * DAY;
const LATE: i64 = DAY;
const INVALID: u32 = u32::MAX;
const PBITS: u32 = 9;
const QBITS: u32 = 9;
const NO_CAT: u8 = 255;
const CHUNK: usize = 4 << 20;

// ------------------------------------------------------------------ records
#[derive(Clone, Copy)]
#[repr(C)]
struct ERec {
    eid: u64,
    person: u64,
    ingest: i64,
    ts: u32,
    pc: u32, // price << 8 | category
}

#[derive(Clone, Copy)]
#[repr(C)]
struct PRec {
    person: u64,
    ts: u32,
    pc: u32,
}

#[derive(Clone, Copy)]
#[repr(C)]
struct CRec {
    person: u64,
    cid: u64,
    ts: u32,
    camp: u32,
}

#[derive(Clone, Copy)]
struct Click {
    ts: u32,
    camp: u32,
    cid: u64,
}

struct URow {
    person: u64,
    day: i32,
    sessions: u32,
    events: u32,
    revenue: i64,
    top: u8,
}

// ------------------------------------------------------------------ files
mod mapped {
    use std::ffi::c_void;
    use std::os::fd::AsRawFd;
    extern "C" {
        fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut c_void;
        fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
    }
    pub struct Map {
        ptr: *const u8,
        len: usize,
    }
    unsafe impl Send for Map {}
    unsafe impl Sync for Map {}
    impl Map {
        pub fn open(path: &std::path::Path) -> Map {
            let f = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
            let len = f.metadata().unwrap().len() as usize;
            if len == 0 {
                return Map { ptr: std::ptr::NonNull::<u8>::dangling().as_ptr(), len: 0 };
            }
            let p = unsafe { mmap(std::ptr::null_mut(), len, 1, 2, f.as_raw_fd(), 0) };
            if p as isize == -1 {
                panic!("mmap {}", path.display());
            }
            unsafe { madvise(p, len, 3) }; // MADV_WILLNEED
            Map { ptr: p as *const u8, len }
        }
        pub fn bytes(&self) -> &[u8] {
            unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
        }
    }
}
use mapped::Map;

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "csv") && p.is_file())
            .collect(),
        Err(_) => Vec::new(),
    };
    v.sort();
    v
}

struct Task {
    file: usize,
    start: usize,
    end: usize,
}

fn chunk_tasks(maps: &[Map]) -> Vec<Task> {
    let mut tasks = Vec::new();
    for (i, m) in maps.iter().enumerate() {
        let len = m.bytes().len();
        let mut s = 0;
        while s < len {
            let e = (s + CHUNK).min(len);
            tasks.push(Task { file: i, start: s, end: e });
            s = e;
        }
    }
    tasks
}

// ------------------------------------------------------------------ parallel helper
fn par<S: Send>(
    nthreads: usize,
    ntasks: usize,
    init: impl Fn(usize) -> S + Sync,
    work: impl Fn(&mut S, usize) + Sync,
) -> Vec<S> {
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        let hs: Vec<_> = (0..nthreads)
            .map(|t| {
                let (next, init, work) = (&next, &init, &work);
                sc.spawn(move || {
                    let mut s = init(t);
                    loop {
                        let i = next.fetch_add(1, Relaxed);
                        if i >= ntasks {
                            break;
                        }
                        work(&mut s, i);
                    }
                    s
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

// ------------------------------------------------------------------ lookups
enum IdMap<V: Copy> {
    /// Every key is `prefix` + a decimal without leading zeros.
    Dense { prefix: &'static [u8], table: Vec<V>, absent: V },
    Hash { map: HashMap<Vec<u8>, V>, absent: V },
}

fn dense_num(s: &[u8], prefix: &[u8]) -> Option<usize> {
    let n = prefix.len();
    if s.len() <= n || s.len() > n + 7 || &s[..n] != prefix || s[n] == b'0' {
        return None;
    }
    let mut v = 0usize;
    for &c in &s[n..] {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as usize;
    }
    Some(v)
}

impl<V: Copy> IdMap<V> {
    fn build(pairs: &[(Vec<u8>, V)], prefix: &'static [u8], absent: V) -> Self {
        if pairs.iter().all(|(k, _)| dense_num(k, prefix).is_some()) {
            let max = pairs.iter().map(|(k, _)| dense_num(k, prefix).unwrap()).max().unwrap_or(0);
            let mut table = vec![absent; max + 1];
            for (k, v) in pairs {
                table[dense_num(k, prefix).unwrap()] = *v;
            }
            IdMap::Dense { prefix, table, absent }
        } else {
            IdMap::Hash { map: pairs.iter().cloned().collect(), absent }
        }
    }
    #[inline(always)]
    fn get(&self, s: &[u8]) -> V {
        match self {
            IdMap::Dense { prefix, table, absent } => match dense_num(s, prefix) {
                Some(n) if n < table.len() => table[n],
                _ => *absent,
            },
            IdMap::Hash { map, absent } => map.get(s).copied().unwrap_or(*absent),
        }
    }
}

fn read_pairs(path: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let data = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let mut out = Vec::new();
    csv::for_lines(&data, 0, data.len(), |buf, p| {
        let mut f = [(0, 0); 2];
        let (next, ok) = csv::split::<2>(buf, p, &mut f);
        if ok && f[0].0 < f[0].1 {
            out.push((buf[f[0].0..f[0].1].to_vec(), buf[f[1].0..f[1].1].to_vec()));
        }
        next
    });
    out
}

struct Ctx {
    ws: i64,
    we: i64,
    base: i64,
    day0: i64,
    ndays: usize,
    cats: Vec<Vec<u8>>,
    products: IdMap<u8>,
    campaigns: IdMap<u32>,
    camp_ids: Vec<Vec<u8>>,
    camp_chan: Vec<Vec<u8>>,
}

fn manifest_value(text: &str, name: &str) -> String {
    let at = text.find(&format!("\"{name}\"")).unwrap_or_else(|| panic!("manifest has no {name}"));
    let rest = &text[at + name.len() + 2..];
    let colon = rest.find(':').unwrap();
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"').unwrap();
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').unwrap();
    rest[..q2].to_string()
}

// ------------------------------------------------------------------ links
struct LinkTable {
    slots: Vec<AtomicU64>, // [key, !name] pairs; key 0 = empty
    parent: Vec<AtomicU32>, // parent + 1; 0 = root
    mask: usize,
    shift: u32,
}

fn zeroed_atomic_u64(n: usize) -> Vec<AtomicU64> {
    let v = std::mem::ManuallyDrop::new(vec![0u64; n]);
    unsafe { Vec::from_raw_parts(v.as_ptr() as *mut AtomicU64, v.len(), v.capacity()) }
}
fn zeroed_atomic_u32(n: usize) -> Vec<AtomicU32> {
    let v = std::mem::ManuallyDrop::new(vec![0u32; n]);
    unsafe { Vec::from_raw_parts(v.as_ptr() as *mut AtomicU32, v.len(), v.capacity()) }
}

impl LinkTable {
    fn new(min_cap: usize) -> Self {
        let cap = min_cap.next_power_of_two().max(1024);
        LinkTable {
            slots: zeroed_atomic_u64(2 * cap),
            parent: zeroed_atomic_u32(cap),
            mask: cap - 1,
            shift: 64 - cap.trailing_zeros(),
        }
    }
    #[inline(always)]
    fn home(&self, k: u64) -> usize {
        (keys::hash(k) >> self.shift) as usize
    }
    fn insert(&self, k: u64) -> u32 {
        let mut i = self.home(k);
        loop {
            let cur = self.slots[2 * i].load(Acquire);
            if cur == k {
                return i as u32;
            }
            if cur == 0 {
                match self.slots[2 * i].compare_exchange(0, k, AcqRel, Acquire) {
                    Ok(_) => return i as u32,
                    Err(actual) if actual == k => return i as u32,
                    Err(_) => {}
                }
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    fn find(&self, mut x: u32) -> u32 {
        loop {
            let p = self.parent[x as usize].load(Relaxed);
            if p == 0 {
                return x;
            }
            let pp = self.parent[(p - 1) as usize].load(Relaxed);
            if pp == 0 {
                return p - 1;
            }
            self.parent[x as usize].store(pp, Relaxed);
            x = pp - 1;
        }
    }
    fn union(&self, a: u32, b: u32) {
        loop {
            let ra = self.find(a);
            let rb = self.find(b);
            if ra == rb {
                return;
            }
            let (hi, lo) = if ra > rb { (ra, rb) } else { (rb, ra) };
            if self.parent[hi as usize].compare_exchange(0, lo + 1, AcqRel, Relaxed).is_ok() {
                return;
            }
        }
    }
    /// The person (name key) of an ID key.
    #[inline(always)]
    fn person(&self, k: u64) -> u64 {
        let mut i = self.home(k);
        loop {
            let cur = self.slots[2 * i].load(Relaxed);
            if cur == k {
                return !self.slots[2 * i + 1].load(Relaxed);
            }
            if cur == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ------------------------------------------------------------------ small parsers
#[inline(always)]
fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    for &c in s {
        if c.wrapping_sub(b'0') > 9 {
            return None;
        }
    }
    let rest = &s[i..];
    if rest.is_empty() || rest.len() > 8 {
        return None;
    }
    let mut v = 0u32;
    for &c in rest {
        v = v * 10 + (c - b'0') as u32;
    }
    if (1..=10_000_000).contains(&v) {
        Some(v)
    } else {
        None
    }
}

// ------------------------------------------------------------------ person map
struct PMap {
    keys: Vec<u64>,
    vals: Vec<u32>,
    mask: usize,
    shift: u32,
    len: usize,
}

impl PMap {
    fn new(expected: usize) -> Self {
        let cap = (expected * 2).next_power_of_two().max(64);
        PMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, shift: 64 - cap.trailing_zeros(), len: 0 }
    }
    #[inline(always)]
    fn home(&self, k: u64) -> usize {
        (keys::hash(k) >> self.shift) as usize
    }
    #[inline(always)]
    fn get_or_insert(&mut self, k: u64) -> u32 {
        let mut i = self.home(k);
        loop {
            let cur = self.keys[i];
            if cur == k {
                return self.vals[i];
            }
            if cur == 0 {
                if (self.len + 1) * 10 > self.keys.len() * 7 {
                    self.grow();
                    return self.get_or_insert(k);
                }
                self.keys[i] = k;
                self.vals[i] = self.len as u32;
                self.len += 1;
                return self.vals[i];
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline(always)]
    fn get(&self, k: u64) -> Option<u32> {
        let mut i = self.home(k);
        loop {
            let cur = self.keys[i];
            if cur == k {
                return Some(self.vals[i]);
            }
            if cur == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let old_k = std::mem::take(&mut self.keys);
        let old_v = std::mem::take(&mut self.vals);
        let cap = old_k.len() * 2;
        self.keys = vec![0; cap];
        self.vals = vec![0; cap];
        self.mask = cap - 1;
        self.shift = 64 - cap.trailing_zeros();
        for (k, v) in old_k.into_iter().zip(old_v) {
            if k != 0 {
                let mut i = self.home(k);
                while self.keys[i] != 0 {
                    i = (i + 1) & self.mask;
                }
                self.keys[i] = k;
                self.vals[i] = v;
            }
        }
    }
}

// ------------------------------------------------------------------ phase D state
struct DState {
    urows: Vec<URow>,
    // per (campaign or organic) x day
    c_sessions: Vec<u64>,
    c_purch: Vec<u64>,
    c_rev: Vec<i64>,
    c_assist: Vec<u64>,
    stamp: Vec<u32>,
    serial: u32,
    catsum: Vec<i64>,
    touched: Vec<u8>,
}

fn sort_events(v: &mut [u64]) {
    if v.len() <= 24 {
        for i in 1..v.len() {
            let x = v[i];
            let mut j = i;
            while j > 0 && v[j - 1] > x {
                v[j] = v[j - 1];
                j -= 1;
            }
            v[j] = x;
        }
    } else {
        v.sort_unstable();
    }
}

fn click_order(a: &Click, b: &Click) -> std::cmp::Ordering {
    // ts ascending; within a ts, the smallest click_id last
    a.ts.cmp(&b.ts).then_with(|| keys::cmp_keys(b.cid, a.cid))
}

#[allow(clippy::too_many_arguments)]
fn process_person(ctx: &Ctx, person: u64, ev: &[u64], ck: &[Click], st: &mut DState) {
    let ndays = ctx.ndays;
    let organic = ctx.camp_ids.len();
    let n = ev.len();
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = usize::MAX;
    let (mut d_sessions, mut d_events, mut d_rev) = (0u32, 0u32, 0i64);
    let mut i = 0;
    let reach = REACH as u32;
    while i < n {
        let start = (ev[i] >> 32) as u32;
        let day = ((ctx.base + start as i64).div_euclid(DAY) - ctx.day0) as usize;
        if day != cur_day {
            if cur_day != usize::MAX {
                flush_day(ctx, person, cur_day, d_sessions, d_events, d_rev, st);
            }
            cur_day = day;
            d_sessions = 0;
            d_events = 0;
            d_rev = 0;
        }
        let mut prev = start;
        let mut j = i;
        let mut rev = 0i64;
        let mut purchased = false;
        while j < n {
            let t = (ev[j] >> 32) as u32;
            if t - prev > GAP {
                break;
            }
            prev = t;
            let pc = ev[j] as u32;
            let price = pc >> 8;
            if price > 0 {
                purchased = true;
                rev += price as i64;
                let c = (pc & 0xFF) as usize;
                if st.catsum[c] == 0 {
                    st.touched.push(c as u8);
                }
                st.catsum[c] += price as i64;
            }
            j += 1;
        }
        d_sessions += 1;
        d_events += (j - i) as u32;
        d_rev += rev;
        // attribution
        while hi < ck.len() && ck[hi].ts <= start {
            hi += 1;
        }
        let low = start - reach;
        while lo < hi && ck[lo].ts < low {
            lo += 1;
        }
        let credited = if lo < hi { ck[hi - 1].camp as usize } else { organic };
        let idx = credited * ndays + day;
        st.c_sessions[idx] += 1;
        st.c_purch[idx] += purchased as u64;
        st.c_rev[idx] += rev;
        if lo < hi {
            st.serial = st.serial.wrapping_add(1);
            if st.serial == 0 {
                st.stamp.iter_mut().for_each(|s| *s = 0);
                st.serial = 1;
            }
            let serial = st.serial;
            for c in &ck[lo..hi] {
                let cc = c.camp as usize;
                if st.stamp[cc] != serial {
                    st.stamp[cc] = serial;
                    st.c_assist[cc * ndays + day] += 1;
                }
            }
        }
        i = j;
    }
    if cur_day != usize::MAX {
        flush_day(ctx, person, cur_day, d_sessions, d_events, d_rev, st);
    }
}

fn flush_day(ctx: &Ctx, person: u64, day: usize, sessions: u32, events: u32, revenue: i64, st: &mut DState) {
    let mut top = NO_CAT;
    let mut best = 0i64;
    for &c in &st.touched {
        let s = st.catsum[c as usize];
        if s > best || (s == best && c < top) {
            best = s;
            top = c;
        }
        st.catsum[c as usize] = 0;
    }
    st.touched.clear();
    st.urows.push(URow { person, day: (ctx.day0 + day as i64) as i32, sessions, events, revenue, top });
}

// ------------------------------------------------------------------ main
fn main() {
    let t0 = Instant::now();
    let lap = |what: &str| eprintln!("{:>8.1} ms  {what}", t0.elapsed().as_secs_f64() * 1000.0);
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let nthreads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()))
        .max(1);

    // ---- lookups
    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let ws = ts::parse(manifest_value(&manifest, "window_start").as_bytes()).expect("window_start");
    let we = ts::parse(manifest_value(&manifest, "window_end").as_bytes()).expect("window_end");
    let base = ws - REACH;
    assert!(we > ws && we - base < (1i64 << 31), "window too long for 32-bit offsets");
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    let prod_pairs = read_pairs(&data.join("products.csv"));
    let mut cats: Vec<Vec<u8>> = prod_pairs.iter().map(|(_, c)| c.clone()).collect();
    cats.push(b"unknown".to_vec());
    cats.sort();
    cats.dedup();
    assert!(cats.len() < NO_CAT as usize, "too many categories");
    let unknown = cats.iter().position(|c| c == b"unknown").unwrap() as u8;
    let cat_index: HashMap<&[u8], u8> = cats.iter().enumerate().map(|(i, c)| (&c[..], i as u8)).collect();
    let prod: Vec<(Vec<u8>, u8)> = prod_pairs.iter().map(|(p, c)| (p.clone(), cat_index[&c[..]])).collect();
    let products = IdMap::build(&prod, b"p", unknown);

    let camp_pairs = read_pairs(&data.join("campaigns.csv"));
    let camp_ids: Vec<Vec<u8>> = camp_pairs.iter().map(|(c, _)| c.clone()).collect();
    let camp_chan: Vec<Vec<u8>> = camp_pairs.iter().map(|(_, ch)| ch.clone()).collect();
    let camp: Vec<(Vec<u8>, u32)> = camp_ids.iter().enumerate().map(|(i, c)| (c.clone(), i as u32)).collect();
    let campaigns = IdMap::build(&camp, b"c_", u32::MAX);
    let ctx = Ctx { ws, we, base, day0, ndays, cats, products, campaigns, camp_ids, camp_chan };

    let ev_files = csv_files(&data.join("events"));
    let ck_files = csv_files(&data.join("clicks"));
    let ln_files = csv_files(&data.join("links"));
    let nev = ev_files.len();
    let nck = ck_files.len();
    let all_files: Vec<PathBuf> = ev_files.iter().chain(&ck_files).chain(&ln_files).cloned().collect();
    let maps: Vec<Map> = {
        let slots: Vec<std::sync::Mutex<Option<Map>>> = all_files.iter().map(|_| std::sync::Mutex::new(None)).collect();
        par(nthreads, all_files.len(), |_| (), |_, i| {
            *slots[i].lock().unwrap() = Some(Map::open(&all_files[i]));
        });
        slots.into_iter().map(|m| m.into_inner().unwrap().unwrap()).collect()
    };
    lap("open");

    // ---- A: links
    let link_maps = &maps[nev + nck..];
    let link_bytes: usize = link_maps.iter().map(|m| m.bytes().len()).sum();
    let links = LinkTable::new(link_bytes / 20 * 2 * 13 / 10 + 1024);
    let link_tasks = chunk_tasks(link_maps);
    par(nthreads, link_tasks.len(), |_| (), |_, ti| {
        let t = &link_tasks[ti];
        let d = link_maps[t.file].bytes();
        csv::for_lines(d, t.start, t.end, |buf, p| {
            let mut f = [(0, 0); 2];
            let (next, ok) = csv::split::<2>(buf, p, &mut f);
            if ok && f[0].0 < f[0].1 && f[1].0 < f[1].1 {
                let a = keys::key(&buf[f[0].0..f[0].1]);
                let b = keys::key(&buf[f[1].0..f[1].1]);
                if a != b {
                    let sa = links.insert(a);
                    let sb = links.insert(b);
                    links.union(sa, sb);
                }
            }
            next
        });
    });
    lap("links: union");
    let cap = links.mask + 1;
    let blocks = 256.min(cap);
    let per = cap / blocks;
    // roots, fully compressed
    par(nthreads, blocks, |_| (), |_, b| {
        for i in b * per..(b + 1) * per {
            if links.slots[2 * i].load(Relaxed) != 0 {
                let r = links.find(i as u32);
                if r != i as u32 {
                    links.parent[i].store(r + 1, Relaxed);
                }
            }
        }
    });
    let root = |i: usize| -> usize {
        let p = links.parent[i].load(Relaxed);
        if p == 0 {
            i
        } else {
            (p - 1) as usize
        }
    };
    if keys::ANY_WEIRD.load(Relaxed) {
        for i in 0..cap {
            let k = links.slots[2 * i].load(Relaxed);
            if k != 0 {
                let r = root(i);
                let cur = links.slots[2 * r + 1].load(Relaxed);
                if cur == 0 || keys::cmp_keys(k, !cur) == std::cmp::Ordering::Less {
                    links.slots[2 * r + 1].store(!k, Relaxed);
                }
            }
        }
    } else {
        par(nthreads, blocks, |_| (), |_, b| {
            for i in b * per..(b + 1) * per {
                let k = links.slots[2 * i].load(Relaxed);
                if k != 0 {
                    links.slots[2 * root(i) + 1].fetch_max(!k, Relaxed);
                }
            }
        });
    }
    par(nthreads, blocks, |_| (), |_, b| {
        for i in b * per..(b + 1) * per {
            if links.slots[2 * i].load(Relaxed) != 0 {
                let r = root(i);
                if r != i {
                    let name = links.slots[2 * r + 1].load(Relaxed);
                    links.slots[2 * i + 1].store(name, Relaxed);
                }
            }
        }
    });
    lap("links: named");

    // ---- B: parse events and clicks
    let ev_maps = &maps[..nev];
    let ck_maps = &maps[nev..nev + nck];
    let ev_tasks = chunk_tasks(ev_maps);
    let ck_tasks = chunk_tasks(ck_maps);
    let ev_bytes: usize = ev_maps.iter().map(|m| m.bytes().len()).sum();
    let ck_bytes: usize = ck_maps.iter().map(|m| m.bytes().len()).sum();
    let np = 1usize << PBITS;
    let nq = 1usize << QBITS;
    let ev_reserve = ev_bytes / 70 / nthreads / np + 64;
    let ck_reserve = ck_bytes / 40 / nthreads / nq + 64;
    struct BState {
        ep: Vec<Vec<ERec>>,
        cp: Vec<Vec<CRec>>,
    }
    let ntasks = ev_tasks.len() + ck_tasks.len();
    let bstates = par(
        nthreads,
        ntasks,
        |_| BState {
            ep: (0..np).map(|_| Vec::with_capacity(ev_reserve)).collect(),
            cp: (0..nq).map(|_| Vec::with_capacity(ck_reserve)).collect(),
        },
        |st, ti| {
            if ti < ev_tasks.len() {
                let t = &ev_tasks[ti];
                let d = ev_maps[t.file].bytes();
                csv::for_lines(d, t.start, t.end, |buf, p| {
                    let mut f = [(0, 0); 7];
                    let (next, ok) = csv::split::<7>(buf, p, &mut f);
                    if !ok || f[0].0 == f[0].1 {
                        return next;
                    }
                    let fld = |i: usize| &buf[f[i].0..f[i].1];
                    let ek = keys::key(fld(0));
                    let ingest = ts::parse(fld(3)).unwrap_or(i64::MAX);
                    let mut tsv = INVALID;
                    let mut pc = 0u32;
                    let mut person = 0u64;
                    'v: {
                        let uid = fld(1);
                        let pid = fld(5);
                        if uid.is_empty() || pid.is_empty() {
                            break 'v;
                        }
                        let ty = fld(4);
                        let purchase = match ty.len() {
                            4 if ty == b"view" || ty == b"cart" => false,
                            8 if ty == b"purchase" => true,
                            _ => break 'v,
                        };
                        let t = match ts::parse(fld(2)) {
                            Some(t) => t,
                            None => break 'v,
                        };
                        if t < ctx.ws || t >= ctx.we || ingest.saturating_sub(t) > LATE {
                            break 'v;
                        }
                        let price = if purchase {
                            match parse_price(fld(6)) {
                                Some(v) => v,
                                None => break 'v,
                            }
                        } else {
                            0
                        };
                        let cat = ctx.products.get(pid);
                        tsv = (t - ctx.base) as u32;
                        pc = (price << 8) | cat as u32;
                        person = links.person(keys::key(uid));
                    }
                    let part = (keys::hash(ek) >> (64 - PBITS)) as usize;
                    st.ep[part].push(ERec { eid: ek, person, ingest, ts: tsv, pc });
                    next
                });
            } else {
                let t = &ck_tasks[ti - ev_tasks.len()];
                let d = ck_maps[t.file].bytes();
                csv::for_lines(d, t.start, t.end, |buf, p| {
                    let mut f = [(0, 0); 4];
                    let (next, ok) = csv::split::<4>(buf, p, &mut f);
                    if !ok || f[0].0 == f[0].1 || f[1].0 == f[1].1 {
                        return next;
                    }
                    let fld = |i: usize| &buf[f[i].0..f[i].1];
                    let camp = ctx.campaigns.get(fld(2));
                    if camp == u32::MAX {
                        return next;
                    }
                    let t = match ts::parse(fld(3)) {
                        Some(t) => t,
                        None => return next,
                    };
                    if t < ctx.base || t >= ctx.we {
                        return next;
                    }
                    let person = links.person(keys::key(fld(1)));
                    let cid = keys::key(fld(0));
                    let part = (keys::hash2(person) >> (64 - QBITS)) as usize;
                    st.cp[part].push(CRec { person, cid, ts: (t - ctx.base) as u32, camp });
                    next
                });
            }
        },
    );
    lap("parse");

    // ---- C: dedup per event_id partition, repartition by person
    struct CState {
        pp: Vec<Vec<PRec>>,
        keys: Vec<u64>,
        best: Vec<(i64, u32, u32)>,
        touched: Vec<u32>,
    }
    let pp_reserve = ev_bytes / 75 / nthreads / nq + 64;
    let cstates = par(
        nthreads,
        np,
        |_| CState {
            pp: (0..nq).map(|_| Vec::with_capacity(pp_reserve)).collect(),
            keys: Vec::new(),
            best: Vec::new(),
            touched: Vec::new(),
        },
        |st, p| {
            let slices: Vec<&[ERec]> = bstates.iter().map(|s| &s.ep[p][..]).collect();
            let n: usize = slices.iter().map(|s| s.len()).sum();
            let tbits = (n * 2).next_power_of_two().max(64).trailing_zeros();
            let cap = 1usize << tbits;
            if st.keys.len() < cap {
                st.keys = vec![0; cap];
                st.best = vec![(0, 0, 0); cap];
            }
            let mask = cap - 1;
            for (si, s) in slices.iter().enumerate() {
                for (ri, r) in s.iter().enumerate() {
                    let h = keys::hash(r.eid);
                    let mut i = ((h << PBITS) >> (64 - tbits)) as usize;
                    loop {
                        let k = st.keys[i];
                        if k == r.eid {
                            if r.ingest < st.best[i].0 {
                                st.best[i] = (r.ingest, si as u32, ri as u32);
                            }
                            break;
                        }
                        if k == 0 {
                            st.keys[i] = r.eid;
                            st.best[i] = (r.ingest, si as u32, ri as u32);
                            st.touched.push(i as u32);
                            break;
                        }
                        i = (i + 1) & mask;
                    }
                }
            }
            let touched = std::mem::take(&mut st.touched);
            for &i in &touched {
                let i = i as usize;
                let (_, si, ri) = st.best[i];
                st.keys[i] = 0;
                let r = &slices[si as usize][ri as usize];
                if r.ts != INVALID {
                    let q = (keys::hash2(r.person) >> (64 - QBITS)) as usize;
                    st.pp[q].push(PRec { person: r.person, ts: r.ts, pc: r.pc });
                }
            }
            st.touched = touched;
            st.touched.clear();
        },
    );
    lap("dedup");

    // ---- D: sessions per person partition
    let ncamp = ctx.camp_ids.len() + 1;
    let dstates = par(
        nthreads,
        nq,
        |_| DState {
            urows: Vec::new(),
            c_sessions: vec![0; ncamp * ndays],
            c_purch: vec![0; ncamp * ndays],
            c_rev: vec![0; ncamp * ndays],
            c_assist: vec![0; ncamp * ndays],
            stamp: vec![0; ncamp],
            serial: 0,
            catsum: vec![0; 256],
            touched: Vec::new(),
        },
        |st, q| {
            let slices: Vec<&[PRec]> = cstates.iter().map(|s| &s.pp[q][..]).collect();
            let n: usize = slices.iter().map(|s| s.len()).sum();
            let mut pmap = PMap::new(n / 8 + 16);
            let mut lid = Vec::with_capacity(n);
            let mut counts: Vec<u32> = Vec::new();
            let mut names: Vec<u64> = Vec::new();
            for s in &slices {
                for r in s.iter() {
                    let l = pmap.get_or_insert(r.person);
                    if l as usize == counts.len() {
                        counts.push(0);
                        names.push(r.person);
                    }
                    counts[l as usize] += 1;
                    lid.push(l);
                }
            }
            let np_ = counts.len();
            let mut off = vec![0u32; np_ + 1];
            for i in 0..np_ {
                off[i + 1] = off[i] + counts[i];
            }
            let mut fill = off.clone();
            let mut ev = vec![0u64; n];
            let mut k = 0;
            for s in &slices {
                for r in s.iter() {
                    let l = lid[k] as usize;
                    k += 1;
                    ev[fill[l] as usize] = ((r.ts as u64) << 32) | r.pc as u64;
                    fill[l] += 1;
                }
            }
            // clicks of these persons
            let cslices: Vec<&[CRec]> = bstates.iter().map(|s| &s.cp[q][..]).collect();
            let mut ccount = vec![0u32; np_ + 1];
            let mut clid: Vec<u32> = Vec::new();
            for s in &cslices {
                for c in s.iter() {
                    let l = pmap.get(c.person).unwrap_or(u32::MAX);
                    clid.push(l);
                    if l != u32::MAX {
                        ccount[l as usize] += 1;
                    }
                }
            }
            let mut coff = vec![0u32; np_ + 1];
            for i in 0..np_ {
                coff[i + 1] = coff[i] + ccount[i];
            }
            let mut cfill = coff.clone();
            let mut ck = vec![Click { ts: 0, camp: 0, cid: 0 }; coff[np_] as usize];
            let mut k = 0;
            for s in &cslices {
                for c in s.iter() {
                    let l = clid[k];
                    k += 1;
                    if l != u32::MAX {
                        let l = l as usize;
                        ck[cfill[l] as usize] = Click { ts: c.ts, camp: c.camp, cid: c.cid };
                        cfill[l] += 1;
                    }
                }
            }
            for l in 0..np_ {
                let e = &mut ev[off[l] as usize..off[l + 1] as usize];
                sort_events(e);
                let c = &mut ck[coff[l] as usize..coff[l + 1] as usize];
                if c.len() > 1 {
                    c.sort_unstable_by(click_order);
                }
                process_person(&ctx, names[l], e, c, st);
            }
        },
    );
    lap("sessions");

    // ---- E: write
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    let nonempty: Vec<usize> = (0..dstates.len()).filter(|&i| !dstates[i].urows.is_empty()).collect();
    let files = if nonempty.is_empty() { vec![0] } else { nonempty };
    par(nthreads, files.len(), |_| (), |_, fi| {
        let rows = &dstates[files[fi]].urows;
        let mut ids = Vec::with_capacity(rows.len() * 10);
        let mut ends = Vec::with_capacity(rows.len());
        let mut tops = Vec::new();
        let mut tend = Vec::new();
        let mut present = Vec::with_capacity(rows.len());
        for r in rows {
            keys::write_id(r.person, &mut ids);
            ends.push(ids.len() as u32);
            if r.top != NO_CAT {
                tops.extend_from_slice(&ctx.cats[r.top as usize]);
                tend.push(tops.len() as u32);
                present.push(true);
            } else {
                present.push(false);
            }
        }
        use parquet::{Column, Logical, Values};
        let cols = vec![
            Column { name: "user_id", logical: Logical::String, values: Values::Str { bytes: ids, ends }, present: None },
            Column { name: "day", logical: Logical::Date, values: Values::I32(rows.iter().map(|r| r.day).collect()), present: None },
            Column { name: "sessions", logical: Logical::None, values: Values::I64(rows.iter().map(|r| r.sessions as i64).collect()), present: None },
            Column { name: "events", logical: Logical::None, values: Values::I64(rows.iter().map(|r| r.events as i64).collect()), present: None },
            Column { name: "revenue_cents", logical: Logical::None, values: Values::I64(rows.iter().map(|r| r.revenue).collect()), present: None },
            Column { name: "top_category", logical: Logical::String, values: Values::Str { bytes: tops, ends: tend }, present: Some(present) },
        ];
        parquet::write(&ud.join(format!("part-{fi:05}.parquet")), rows.len(), &cols).expect("write user_daily");
    });
    {
        let mut cs = vec![0u64; ncamp * ndays];
        let mut cp = vec![0u64; ncamp * ndays];
        let mut cr = vec![0i64; ncamp * ndays];
        let mut ca = vec![0u64; ncamp * ndays];
        for s in &dstates {
            for i in 0..ncamp * ndays {
                cs[i] += s.c_sessions[i];
                cp[i] += s.c_purch[i];
                cr[i] += s.c_rev[i];
                ca[i] += s.c_assist[i];
            }
        }
        let (mut idb, mut ide, mut chb, mut che) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (mut day, mut vs, mut vp, mut vr, mut va) = (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for c in 0..ncamp {
            for d in 0..ndays {
                let i = c * ndays + d;
                if cs[i] == 0 && ca[i] == 0 {
                    continue;
                }
                if c + 1 == ncamp {
                    idb.extend_from_slice(b"organic");
                    chb.extend_from_slice(b"organic");
                } else {
                    idb.extend_from_slice(&ctx.camp_ids[c]);
                    chb.extend_from_slice(&ctx.camp_chan[c]);
                }
                ide.push(idb.len() as u32);
                che.push(chb.len() as u32);
                day.push((ctx.day0 + d as i64) as i32);
                vs.push(cs[i] as i64);
                vp.push(cp[i] as i64);
                vr.push(cr[i]);
                va.push(ca[i] as i64);
            }
        }
        let rows = day.len();
        use parquet::{Column, Logical, Values};
        let cols = vec![
            Column { name: "campaign_id", logical: Logical::String, values: Values::Str { bytes: idb, ends: ide }, present: None },
            Column { name: "channel", logical: Logical::String, values: Values::Str { bytes: chb, ends: che }, present: None },
            Column { name: "day", logical: Logical::Date, values: Values::I32(day), present: None },
            Column { name: "sessions", logical: Logical::None, values: Values::I64(vs), present: None },
            Column { name: "purchasing_sessions", logical: Logical::None, values: Values::I64(vp), present: None },
            Column { name: "revenue_cents", logical: Logical::None, values: Values::I64(vr), present: None },
            Column { name: "assisted_sessions", logical: Logical::None, values: Values::I64(va), present: None },
        ];
        parquet::write(&cd.join("part-00000.parquet"), rows, &cols).expect("write campaign_daily");
    }
    lap("write");
    // The OS reclaims everything; skip destructors of the big buffers.
    std::process::exit(0);
}
