//! Shopping Sessions pipeline: clean -> link IDs -> sessionize -> attribute ->
//! assists -> two daily Parquet tables. See rules.md.
//!
//! Layout: every ID is reduced to a 128-bit xxh3 key at parse time, so later
//! stages never chase pointers back into the input. Work moves between stages
//! by hash partition (event id -> user id -> person), so every lookup table a
//! worker touches is partition-local and cache-sized.

mod output;
mod parse;

use hashbrown::hash_table::Entry;
use hashbrown::{HashMap, HashTable};
use memchr::memchr;
use parse::*;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_128;

/// Partition count (power of two) for hash-partitioned stages.
const NPART: usize = 256;
/// Target size of one parse task.
const CHUNK: usize = 4 << 20;

/// A 128-bit ID key (two u64 halves keep the alignment at 8).
#[derive(Clone, Copy, PartialEq, Eq, Default, Debug)]
pub struct K(u64, u64);

#[inline]
fn key(s: &[u8]) -> K {
    let h = xxh3_128(s);
    K(h as u64, (h >> 64) as u64)
}

impl K {
    /// Hash for hashbrown: low bits pick the slot, top 7 bits are the tag.
    #[inline]
    fn h(self) -> u64 {
        self.0
    }
    /// Partition from bits hashbrown does not lean on.
    #[inline]
    fn part(self) -> usize {
        (self.1 as usize) & (NPART - 1)
    }
}

// ---------------------------------------------------------------- logging

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>8.3}s] {} {}", $t0.elapsed().as_secs_f64(), format!($($arg)*), mem_stats())
    };
}

static PROF: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
const PROF_NAMES: [&str; 8] = ["links", "parse_ev", "dedup", "parse_ck", "dedup_tab", "group", "sort", "person"];
#[inline]
fn prof(i: usize, t: Instant) {
    PROF[i].fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
}
fn prof_report() -> String {
    (0..8)
        .map(|i| format!("{}={:.2}", PROF_NAMES[i], PROF[i].load(Relaxed) as f64 / 1e9))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Peak RSS and page-fault counts (Linux only; empty elsewhere).
fn mem_stats() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let hwm = status.lines().find(|l| l.starts_with("VmHWM")).unwrap_or("").replace([' ', '\t'], "");
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    if f.len() > 10 {
        format!("| {hwm} minflt={}", f[7])
    } else {
        String::new()
    }
}

// ---------------------------------------------------------------- files

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            let name = e.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') || !name.ends_with(".csv") {
                continue;
            }
            if p.is_file() {
                v.push(p);
            }
        }
    }
    v.sort();
    v
}

fn map_file(p: &Path) -> S {
    let f = std::fs::File::open(p).unwrap_or_else(|e| panic!("open {}: {e}", p.display()));
    let len = f.metadata().map(|m| m.len()).unwrap_or(0);
    if len == 0 {
        return &[];
    }
    // Asynchronous readahead of the whole file, so disk reads overlap parsing
    // (SS_POPULATE=1 instead pre-faults the mapping synchronously).
    let populate = std::env::var("SS_POPULATE").is_ok_and(|v| v == "1");
    let mut opts = memmap2::MmapOptions::new();
    if populate {
        opts.populate();
    }
    let m = unsafe { opts.map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    #[cfg(unix)]
    if !populate {
        let _ = m.advise(memmap2::Advice::WillNeed);
    }
    let m: &'static memmap2::Mmap = Box::leak(Box::new(m));
    &m[..]
}

/// A slice of whole data lines from one file, with that file's column map.
struct Chunk {
    data: S,
    map: [u8; 8],
}

fn make_chunks(files: &[S], wanted: &[&str]) -> Vec<Chunk> {
    let mut out = Vec::new();
    for &f in files {
        if f.is_empty() {
            continue;
        }
        let (header, body) = match memchr(b'\n', f) {
            Some(p) => (&f[..p], &f[p + 1..]),
            None => (f, &f[f.len()..]),
        };
        let cm = column_map(header, wanted);
        let mut map = [0u8; 8];
        for (i, &c) in cm.iter().enumerate() {
            map[i] = c as u8;
        }
        let mut start = 0;
        while start < body.len() {
            let mut end = (start + CHUNK).min(body.len());
            if end < body.len() {
                end = match memchr(b'\n', &body[end..]) {
                    Some(p) => end + p + 1,
                    None => body.len(),
                };
            }
            out.push(Chunk { data: &body[start..end], map });
            start = end;
        }
    }
    out
}

/// Positions of every `,` `"` and `\n` in a buffer, found 64 bytes at a time
/// with SSE2 compare masks (memchr3 fallback off x86_64).
struct Structurals<'a> {
    data: &'a [u8],
    base: usize,
    mask: u64,
    #[cfg(not(target_arch = "x86_64"))]
    it: memchr::arch::all::memchr::ThreeIter<'a, 'a>,
}

impl<'a> Structurals<'a> {
    #[inline(always)]
    fn new(data: &'a [u8]) -> Self {
        let mut s = Structurals {
            data,
            base: 0,
            mask: 0,
            #[cfg(not(target_arch = "x86_64"))]
            it: memchr::arch::all::memchr::Three::new(b',', b'"', b'\n').iter(data),
        };
        #[cfg(target_arch = "x86_64")]
        {
            s.mask = s.block(0);
        }
        s
    }

    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    fn block(&self, base: usize) -> u64 {
        let len = self.data.len();
        if base >= len {
            return 0;
        }
        if base + 64 <= len {
            use std::arch::x86_64::*;
            unsafe {
                let p = self.data.as_ptr().add(base);
                let c = _mm_set1_epi8(b',' as i8);
                let q = _mm_set1_epi8(b'"' as i8);
                let n = _mm_set1_epi8(b'\n' as i8);
                let mut m = 0u64;
                for k in 0..4 {
                    let v = _mm_loadu_si128(p.add(16 * k) as *const __m128i);
                    let r = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(v, c), _mm_cmpeq_epi8(v, q)), _mm_cmpeq_epi8(v, n));
                    m |= (_mm_movemask_epi8(r) as u32 as u64) << (16 * k);
                }
                m
            }
        } else {
            let mut m = 0u64;
            for (k, &b) in self.data[base..].iter().enumerate() {
                if b == b',' || b == b'"' || b == b'\n' {
                    m |= 1 << k;
                }
            }
            m
        }
    }
}

impl Iterator for Structurals<'_> {
    type Item = usize;
    #[cfg(target_arch = "x86_64")]
    #[inline(always)]
    fn next(&mut self) -> Option<usize> {
        while self.mask == 0 {
            self.base += 64;
            if self.base >= self.data.len() {
                return None;
            }
            self.mask = self.block(self.base);
        }
        let pos = self.base + self.mask.trailing_zeros() as usize;
        self.mask &= self.mask - 1;
        Some(pos)
    }
    #[cfg(not(target_arch = "x86_64"))]
    #[inline(always)]
    fn next(&mut self) -> Option<usize> {
        self.it.next()
    }
}

/// One SIMD pass over a chunk: calls `row` with the N unquoted fields of every
/// well-formed line (rules.md "CSV dialect"); malformed lines are skipped.
#[inline(always)]
fn scan_rows<const N: usize>(data: S, mut row: impl FnMut(&[S; N])) {
    let mut fields: [S; N] = [&[]; N];
    let mut nf = 0usize;
    let mut bad = false;
    let mut fs = 0usize; // current field start
    let mut q = 0u32; // quotes seen in current field
    let mut qf = 0usize; // first quote position
    let mut ql = 0usize; // last quote position
    let mut line_start = 0usize;
    macro_rules! close_field {
        ($fe:expr) => {{
            let fe = $fe;
            if nf < N {
                if q == 0 {
                    fields[nf] = &data[fs..fe];
                } else if q == 2 && qf == fs && ql + 1 == fe {
                    fields[nf] = &data[fs + 1..fe - 1];
                } else {
                    bad = true;
                }
            }
            nf += 1;
            q = 0;
        }};
    }
    for pos in Structurals::new(data) {
        match data[pos] {
            b',' => {
                close_field!(pos);
                fs = pos + 1;
            }
            b'"' => {
                if q == 0 {
                    qf = pos;
                }
                ql = pos;
                q += 1;
            }
            _ => {
                close_field!(pos);
                if !bad && nf == N {
                    row(&fields);
                }
                nf = 0;
                bad = false;
                fs = pos + 1;
                line_start = pos + 1;
            }
        }
    }
    if line_start < data.len() {
        close_field!(data.len());
        if !bad && nf == N {
            row(&fields);
        }
    }
}

fn read_small_table(path: &Path, wanted: &[&str]) -> Vec<[S; 2]> {
    let f = if path.is_file() { map_file(path) } else { &[] };
    let mut rows = Vec::new();
    for c in make_chunks(&[f], wanted) {
        scan_rows::<2>(c.data, |fl| rows.push([fl[c.map[0] as usize], fl[c.map[1] as usize]]));
    }
    rows
}

fn transpose<T>(v: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..NPART).map(|_| Vec::with_capacity(v.len())).collect();
    for t in v {
        for (p, x) in t.into_iter().enumerate() {
            out[p].push(x);
        }
    }
    out
}

fn new_parts<T>(cap: usize) -> Vec<Vec<T>> {
    (0..NPART).map(|_| Vec::with_capacity(cap)).collect()
}

// ---------------------------------------------------------------- links

/// Every linked ID -> its person's name, as a flat open-addressing table split
/// into one region per partition (so regions build in parallel). Lookups can
/// be prefetched in batches.
struct Links {
    rbits: u32,
    keys: Vec<K>,
    gid: Vec<u32>, // u32::MAX = empty slot; else index into `names` of the person's name
    names: Vec<(K, S)>,
}

impl Links {
    #[inline(always)]
    fn slot(&self, k: K) -> usize {
        (k.part() << self.rbits) | (k.0 as usize & ((1 << self.rbits) - 1))
    }
    #[inline(always)]
    fn prefetch(&self, k: K) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            let s = self.slot(k);
            _mm_prefetch(self.keys.as_ptr().add(s) as *const i8, _MM_HINT_T0);
            _mm_prefetch(self.gid.as_ptr().add(s) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline(always)]
    fn prefetch_name(&self, g: u32) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::*;
            _mm_prefetch(self.names.as_ptr().add(g as usize) as *const i8, _MM_HINT_T0);
        }
    }
    /// Index of the person's name, or u32::MAX if the ID is in no link.
    #[inline(always)]
    fn find(&self, k: K) -> u32 {
        let base = k.part() << self.rbits;
        let m = (1usize << self.rbits) - 1;
        let mut i = k.0 as usize & m;
        loop {
            let g = self.gid[base | i];
            if g == u32::MAX || self.keys[base | i] == k {
                return g;
            }
            i = (i + 1) & m;
        }
    }
    #[inline]
    fn person(&self, k: K) -> Option<(K, S)> {
        match self.find(k) {
            u32::MAX => None,
            g => Some(self.names[g as usize]),
        }
    }
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = parent[x as usize];
    }
    x
}

fn build_links(files: &[S]) -> Links {
    let chunks = make_chunks(files, &["user_id_a", "user_id_b"]);
    // Parse; each task emits its pairs' sides scattered by partition.
    let per_task: Vec<(usize, Vec<Vec<(K, u32, S)>>)> = chunks
        .par_iter()
        .map(|c| {
            let mut out: Vec<Vec<(K, u32, S)>> = new_parts(0);
            let mut n = 0u32;
            scan_rows::<2>(c.data, |f| {
                let a = f[c.map[0] as usize];
                let b = f[c.map[1] as usize];
                if a.is_empty() || b.is_empty() {
                    return;
                }
                let ka = key(a);
                let kb = key(b);
                out[ka.part()].push((ka, 2 * n, a));
                out[kb.part()].push((kb, 2 * n + 1, b));
                n += 1;
            });
            (n as usize, out)
        })
        .collect();
    let mut base = vec![0u32; per_task.len() + 1];
    for (i, (n, _)) in per_task.iter().enumerate() {
        base[i + 1] = base[i] + *n as u32;
    }
    let npairs = base[per_task.len()] as usize;
    // Rebase side keys to global pair numbers, then transpose to partitions.
    let tasks: Vec<Vec<Vec<(K, u32, S)>>> = per_task
        .into_iter()
        .enumerate()
        .map(|(t, (_, mut v))| {
            let b = base[t] * 2;
            if b != 0 {
                for p in v.iter_mut() {
                    for x in p.iter_mut() {
                        x.1 += b;
                    }
                }
            }
            v
        })
        .collect();
    // Intern each partition.
    let interned: Vec<(HashTable<u32>, Vec<K>, Vec<S>, Vec<(u32, u32)>)> = transpose(tasks)
        .into_par_iter()
        .map(|vs| {
            let n: usize = vs.iter().map(|v| v.len()).sum();
            let mut table: HashTable<u32> = HashTable::with_capacity(n);
            let mut ids: Vec<K> = Vec::with_capacity(n);
            let mut strs: Vec<S> = Vec::with_capacity(n);
            let mut assign = Vec::with_capacity(n);
            for v in vs {
                for (k, side, s) in v {
                    let id = match table.entry(k.h(), |&i| ids[i as usize] == k, |&i| ids[i as usize].h()) {
                        Entry::Occupied(o) => *o.get(),
                        Entry::Vacant(vac) => {
                            let id = ids.len() as u32;
                            vac.insert(id);
                            ids.push(k);
                            strs.push(s);
                            id
                        }
                    };
                    assign.push((side, id));
                }
            }
            (table, ids, strs, assign)
        })
        .collect();
    let mut offs = vec![0u32; NPART + 1];
    for (b, x) in interned.iter().enumerate() {
        offs[b + 1] = offs[b] + x.1.len() as u32;
    }
    let nids = offs[NPART] as usize;
    let mut side = vec![0u32; npairs * 2];
    for (b, x) in interned.iter().enumerate() {
        for &(k, id) in &x.3 {
            side[k as usize] = offs[b] + id;
        }
    }
    let mut all: Vec<(K, S)> = Vec::with_capacity(nids);
    for x in &interned {
        all.extend(x.1.iter().copied().zip(x.2.iter().copied()));
    }
    // Union-find over every linked pair.
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    for p in 0..npairs {
        let a = find(&mut parent, side[2 * p]);
        let b = find(&mut parent, side[2 * p + 1]);
        if a != b {
            if a < b {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
    }
    drop(side);
    // The byte-wise smallest ID of each component names it.
    let mut root = vec![0u32; nids];
    let mut best: Vec<u32> = (0..nids as u32).collect();
    for i in 0..nids as u32 {
        let r = find(&mut parent, i);
        root[i as usize] = r;
        if all[i as usize].1 < all[best[r as usize] as usize].1 {
            best[r as usize] = i;
        }
    }
    // Flat lookup table: one region per partition, each at most half full.
    let maxr = interned.iter().map(|x| x.1.len()).max().unwrap_or(0);
    let rbits = ((maxr * 2).max(2)).next_power_of_two().trailing_zeros();
    let rsize = 1usize << rbits;
    let mut keys = vec![K::default(); rsize * NPART];
    let mut gid = vec![u32::MAX; rsize * NPART];
    keys.par_chunks_mut(rsize)
        .zip(gid.par_chunks_mut(rsize))
        .zip(interned.par_iter())
        .enumerate()
        .for_each(|(b, ((kr, gr), x))| {
            for (i, &k) in x.1.iter().enumerate() {
                let mut j = k.0 as usize & (rsize - 1);
                while gr[j] != u32::MAX {
                    j = (j + 1) & (rsize - 1);
                }
                kr[j] = k;
                gr[j] = best[root[offs[b] as usize + i] as usize];
            }
        });
    Links { rbits, keys, gid, names: all }
}

// ---------------------------------------------------------------- events & clicks

const K_PURCHASE: u8 = 3;

/// One parsed copy of an event. kind == 0 means the copy fails validation.
#[derive(Clone, Copy)]
struct Ev {
    eid: K,
    user: K,
    user_s: S,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A surviving event keyed by its user (then, after linking, by its person).
#[derive(Clone, Copy)]
struct UEv {
    who: K,
    who_s: S,
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

/// A valid click keyed by its user (then by its person).
#[derive(Clone, Copy)]
struct UCk {
    who: K,
    ts: i64,
    cid: S,
    camp: u32,
}

struct Ctx {
    ws: i64,
    we: i64,
    products: HashMap<S, u16>,
    campaigns: HashMap<S, u32>,
    unknown_cat: u16,
    ncamp: u32,
    day0: i64,
    ndays: usize,
}

fn parse_events(c: &Chunk, ctx: &Ctx) -> Vec<Vec<Ev>> {
    let t = Instant::now();
    let mut out: Vec<Vec<Ev>> = new_parts(c.data.len() / 72 / NPART + 4);
    let m = c.map;
    let mut tc = TsCache::default();
    scan_rows::<7>(c.data, |f| {
        let eid = f[m[0] as usize];
        if eid.is_empty() {
            return;
        }
        let ingest = tc.parse(f[m[3] as usize]);
        let mut kind = match f[m[4] as usize] {
            b"view" => 1,
            b"cart" => 2,
            b"purchase" => K_PURCHASE,
            _ => 0,
        };
        let user_s = f[m[1] as usize];
        let product = f[m[5] as usize];
        let mut ts = 0;
        let mut price = 0;
        let mut cat = 0;
        if kind != 0 && (ingest.is_none() || user_s.is_empty() || product.is_empty()) {
            kind = 0;
        }
        if kind != 0 {
            match tc.parse(f[m[2] as usize]) {
                // Late: ingest_ts - ts over 24 h. (ts is in the window, no overflow.)
                Some(t) if t >= ctx.ws && t < ctx.we && ingest.unwrap() - t <= DAY => ts = t,
                _ => kind = 0,
            }
        }
        if kind == K_PURCHASE {
            match parse_price(f[m[6] as usize]) {
                Some(p) => {
                    price = p;
                    cat = *ctx.products.get(product).unwrap_or(&ctx.unknown_cat);
                }
                None => kind = 0,
            }
        }
        let user = if kind != 0 { key(user_s) } else { K::default() };
        let ek = key(eid);
        out[ek.part()].push(Ev { eid: ek, user, user_s, ingest: ingest.unwrap_or(i64::MAX), ts, price, cat, kind });
    });
    prof(1, t);
    out
}

/// Dedup one event-id partition: the copy with the smallest ingest_ts wins,
/// before validation. Winners are kept as locations (no copying); valid ones
/// are resolved to their person and routed by person partition.
fn dedup_events(parts: Vec<Vec<Ev>>, links: &Links) -> Vec<Vec<UEv>> {
    let t = Instant::now();
    let n: usize = parts.iter().map(|v| v.len()).sum();
    let mut win: Vec<(u32, u32)> = Vec::with_capacity(n);
    let mut table: HashTable<u32> = HashTable::with_capacity(n);
    {
        let get = |w: (u32, u32)| &parts[w.0 as usize][w.1 as usize];
        for (vi, v) in parts.iter().enumerate() {
            for (pi, e) in v.iter().enumerate() {
                let loc = (vi as u32, pi as u32);
                match table.entry(e.eid.h(), |&i| get(win[i as usize]).eid == e.eid, |&i| get(win[i as usize]).eid.h()) {
                    Entry::Occupied(o) => {
                        let i = *o.get() as usize;
                        if e.ingest < get(win[i]).ingest {
                            win[i] = loc;
                        }
                    }
                    Entry::Vacant(vac) => {
                        vac.insert(win.len() as u32);
                        win.push(loc);
                    }
                }
            }
        }
    }
    drop(table);
    prof(4, t);
    let mut out: Vec<Vec<UEv>> = new_parts(n / NPART + 4);
    const B: usize = 32;
    let mut g = [0u32; B];
    for batch in win.chunks(B) {
        for &(vi, pi) in batch {
            let e = &parts[vi as usize][pi as usize];
            if e.kind != 0 {
                links.prefetch(e.user);
            }
        }
        for (j, &(vi, pi)) in batch.iter().enumerate() {
            let e = &parts[vi as usize][pi as usize];
            if e.kind != 0 {
                g[j] = links.find(e.user);
                if g[j] != u32::MAX {
                    links.prefetch_name(g[j]);
                }
            }
        }
        for (j, &(vi, pi)) in batch.iter().enumerate() {
            let e = &parts[vi as usize][pi as usize];
            if e.kind == 0 {
                continue;
            }
            let (nk, ns) = if g[j] == u32::MAX { (e.user, e.user_s) } else { links.names[g[j] as usize] };
            out[nk.part()].push(UEv { who: nk, who_s: ns, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind });
        }
    }
    prof(2, t);
    out
}

/// Parse and validate clicks, resolved to their person. Duplicate copies of a
/// click carry identical values, so keeping them changes neither the credited
/// click nor the assists.
fn parse_clicks(c: &Chunk, ctx: &Ctx, links: &Links) -> Vec<Vec<UCk>> {
    let t = Instant::now();
    let mut out: Vec<Vec<UCk>> = new_parts(c.data.len() / 50 / NPART + 4);
    let m = c.map;
    let lo = ctx.ws - WEEK;
    let mut tc = TsCache::default();
    // Valid clicks wait in a small batch so their link lookups can be prefetched.
    const B: usize = 32;
    let mut buf: Vec<UCk> = Vec::with_capacity(B);
    let mut g = [0u32; B];
    let flush = |buf: &mut Vec<UCk>, g: &mut [u32; B], out: &mut Vec<Vec<UCk>>| {
        for (j, c) in buf.iter().enumerate() {
            g[j] = links.find(c.who);
            if g[j] != u32::MAX {
                links.prefetch_name(g[j]);
            }
        }
        for (j, mut c) in buf.drain(..).enumerate() {
            if g[j] != u32::MAX {
                c.who = links.names[g[j] as usize].0;
            }
            out[c.who.part()].push(c);
        }
    };
    scan_rows::<4>(c.data, |f| {
        let cid = f[m[0] as usize];
        let user = f[m[1] as usize];
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let ts = match tc.parse(f[m[3] as usize]) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(f[m[2] as usize]) {
            Some(&c) => c,
            None => return,
        };
        let k = key(user);
        links.prefetch(k);
        buf.push(UCk { who: k, ts, cid, camp });
        if buf.len() == B {
            flush(&mut buf, &mut g, &mut out);
        }
    });
    flush(&mut buf, &mut g, &mut out);
    prof(3, t);
    out
}

// ---------------------------------------------------------------- sessions

#[derive(Clone, Copy, Default)]
struct E2 {
    ts: i64,
    price: u32,
    cat: u16,
    kind: u8,
}

#[derive(Clone, Copy)]
struct C2 {
    ts: i64,
    cid: S,
    camp: u32,
}

pub struct URow {
    pub name: S,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: u32, // u32::MAX = null
}

/// Per (campaign, day): sessions, purchasing_sessions, revenue, assisted.
struct CampAcc {
    ndays: usize,
    dense: Vec<[i64; 4]>,
}

impl CampAcc {
    fn new(ctx: &Ctx) -> Self {
        CampAcc { ndays: ctx.ndays, dense: vec![[0; 4]; (ctx.ncamp as usize + 1) * ctx.ndays] }
    }
    #[inline]
    fn at(&mut self, camp: u32, dayi: usize) -> &mut [i64; 4] {
        &mut self.dense[camp as usize * self.ndays + dayi]
    }
    fn merge(mut self, o: CampAcc) -> CampAcc {
        for (a, b) in self.dense.iter_mut().zip(o.dense.iter()) {
            for k in 0..4 {
                a[k] += b[k];
            }
        }
        self
    }
}

#[derive(Default)]
struct Scratch {
    starts: Vec<i64>,
    dayis: Vec<usize>,
    cats: Vec<(u16, i64)>,
    by_camp: Vec<(u32, i64)>,
}

fn flush_day(name: S, day: i64, ur: &mut [i64; 3], cats: &mut Vec<(u16, i64)>, rank: &[u32], rows: &mut Vec<URow>) {
    let mut top = u32::MAX;
    let mut top_sum = 0i64;
    for &(c, s) in cats.iter() {
        let c = c as u32;
        if top == u32::MAX || s > top_sum || (s == top_sum && rank[c as usize] < rank[top as usize]) {
            top = c;
            top_sum = s;
        }
    }
    rows.push(URow { name, day: day as i32, sessions: ur[0], events: ur[1], revenue: ur[2], top });
    cats.clear();
    *ur = [0; 3];
}

#[allow(clippy::too_many_arguments)]
fn person(name: S, es: &[E2], cs: &[C2], ctx: &Ctx, rank: &[u32], acc: &mut CampAcc, rows: &mut Vec<URow>, sc: &mut Scratch) {
    let organic = ctx.ncamp;
    sc.starts.clear();
    sc.dayis.clear();
    sc.cats.clear();
    let mut ur = [0i64; 3]; // sessions, events, revenue for the current day
    let mut cur_day: i64 = i64::MIN;
    let mut i = 0;
    let n = es.len();
    while i < n {
        let start = es[i].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(name, cur_day, &mut ur, &mut sc.cats, rank, rows);
            }
            cur_day = day;
        }
        let mut last = start;
        let mut nev = 0i64;
        let mut rev = 0i64;
        let mut purch = false;
        // Gap over 30 min starts a new session; equal timestamps share one.
        while i < n && es[i].ts - last <= GAP {
            let e = es[i];
            last = e.ts;
            nev += 1;
            if e.kind == K_PURCHASE {
                purch = true;
                rev += e.price as i64;
                match sc.cats.iter_mut().find(|x| x.0 == e.cat) {
                    Some(x) => x.1 += e.price as i64,
                    None => sc.cats.push((e.cat, e.price as i64)),
                }
            }
            i += 1;
        }
        ur[0] += 1;
        ur[1] += nev;
        ur[2] += rev;
        // Credit: latest click at or before start within 7 days; tie -> smallest click_id.
        let mut camp = organic;
        let j = cs.partition_point(|c| c.ts <= start);
        if j > 0 {
            let t = cs[j - 1].ts;
            if start - t <= WEEK {
                let k = cs[..j].partition_point(|c| c.ts < t);
                camp = cs[k].camp;
            }
        }
        let dayi = (day - ctx.day0) as usize;
        let a = acc.at(camp, dayi);
        a[0] += 1;
        a[1] += purch as i64;
        a[2] += rev;
        sc.starts.push(start);
        sc.dayis.push(dayi);
    }
    if cur_day != i64::MIN {
        flush_day(name, cur_day, &mut ur, &mut sc.cats, rank, rows);
    }
    // Assists: campaign c assists a session when one of its clicks lies in
    // [start - 7d, start], i.e. start lies in the union of [ts, ts + 7d].
    if cs.is_empty() {
        return;
    }
    sc.by_camp.clear();
    sc.by_camp.extend(cs.iter().map(|c| (c.camp, c.ts)));
    sc.by_camp.sort_unstable();
    let starts = &sc.starts;
    let dayis = &sc.dayis;
    let mut emit = |camp: u32, l: i64, r: i64| {
        let lo = starts.partition_point(|&s| s < l);
        let hi = starts.partition_point(|&s| s <= r);
        for k in lo..hi {
            acc.at(camp, dayis[k])[3] += 1;
        }
    };
    let bc = &sc.by_camp;
    let mut g = 0;
    while g < bc.len() {
        let camp = bc[g].0;
        let mut l = bc[g].1;
        let mut r = l + WEEK;
        g += 1;
        while g < bc.len() && bc[g].0 == camp {
            let t = bc[g].1;
            if t <= r {
                r = r.max(t + WEEK);
            } else {
                emit(camp, l, r);
                l = t;
                r = t + WEEK;
            }
            g += 1;
        }
        emit(camp, l, r);
    }
}

fn process_partition(evs: Vec<Vec<UEv>>, cks: Vec<Vec<UCk>>, ctx: &Ctx, rank: &[u32], acc: &mut CampAcc) -> Vec<URow> {
    let t = Instant::now();
    let n: usize = evs.iter().map(|v| v.len()).sum();
    let mut rows = Vec::new();
    if n == 0 {
        return rows;
    }
    // Group events by person (counting sort on a local person index).
    let mut names: Vec<(K, S)> = Vec::new();
    let mut cnt: Vec<u32> = Vec::new();
    let mut table: HashTable<u32> = HashTable::with_capacity(n / 4 + 16);
    let mut pid: Vec<u32> = Vec::with_capacity(n);
    let mut last: (K, u32) = (K(u64::MAX, u64::MAX), u32::MAX);
    for v in &evs {
        for e in v {
            let id = if last.1 != u32::MAX && last.0 == e.who {
                last.1
            } else {
                let id = match table.entry(e.who.h(), |&i| names[i as usize].0 == e.who, |&i| names[i as usize].0.h()) {
                    Entry::Occupied(o) => *o.get(),
                    Entry::Vacant(vac) => {
                        let id = names.len() as u32;
                        vac.insert(id);
                        names.push((e.who, e.who_s));
                        cnt.push(0);
                        id
                    }
                };
                last = (e.who, id);
                id
            };
            cnt[id as usize] += 1;
            pid.push(id);
        }
    }
    let np = names.len();
    let mut off = vec![0u32; np + 1];
    for p in 0..np {
        off[p + 1] = off[p] + cnt[p];
    }
    drop(cnt);
    let mut pos = off.clone();
    let mut arr = vec![E2::default(); n];
    let mut k = 0;
    for v in evs {
        for e in v {
            let id = pid[k] as usize;
            k += 1;
            arr[pos[id] as usize] = E2 { ts: e.ts, price: e.price, cat: e.cat, kind: e.kind };
            pos[id] += 1;
        }
    }
    drop(pid);
    // Clicks of people with events (other clicks can credit nothing).
    let mut ccnt = vec![0u32; np];
    let mut cpid: Vec<u32> = Vec::new();
    for v in &cks {
        for c in v {
            let id = match table.find(c.who.h(), |&i| names[i as usize].0 == c.who) {
                Some(&i) => {
                    ccnt[i as usize] += 1;
                    i
                }
                None => u32::MAX,
            };
            cpid.push(id);
        }
    }
    drop(table);
    let mut coff = vec![0u32; np + 1];
    for p in 0..np {
        coff[p + 1] = coff[p] + ccnt[p];
    }
    let mut cpos = coff.clone();
    let mut carr: Vec<C2> = vec![C2 { ts: 0, cid: &[], camp: 0 }; coff[np] as usize];
    let mut k = 0;
    for v in cks {
        for c in v {
            let id = cpid[k];
            k += 1;
            if id != u32::MAX {
                carr[cpos[id as usize] as usize] = C2 { ts: c.ts, cid: c.cid, camp: c.camp };
                cpos[id as usize] += 1;
            }
        }
    }
    prof(5, t);
    let t = Instant::now();
    for p in 0..np {
        arr[off[p] as usize..off[p + 1] as usize].sort_unstable_by_key(|e| e.ts);
        carr[coff[p] as usize..coff[p + 1] as usize]
            .sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp(b.cid)));
    }
    prof(6, t);
    let t = Instant::now();
    let mut sc = Scratch::default();
    for p in 0..np {
        let es = &arr[off[p] as usize..off[p + 1] as usize];
        let cs = &carr[coff[p] as usize..coff[p + 1] as usize];
        person(names[p].1, es, cs, ctx, rank, acc, &mut rows, &mut sc);
    }
    prof(7, t);
    rows
}

// ---------------------------------------------------------------- main

fn manifest_ts(text: &str, key: &str) -> i64 {
    let at = text.find(&format!("\"{key}\"")).unwrap_or_else(|| panic!("manifest: no {key}"));
    let rest = &text[at + key.len() + 2..];
    let colon = rest.find(':').expect("manifest: no colon");
    let rest = &rest[colon + 1..];
    let q1 = rest.find('"').expect("manifest: no value");
    let rest = &rest[q1 + 1..];
    let q2 = rest.find('"').expect("manifest: unterminated value");
    parse_ts(rest[..q2].as_bytes()).unwrap_or_else(|| panic!("manifest: bad {key}"))
}

fn main() {
    let t0 = Instant::now();
    if let Ok(mb) = std::env::var("FAULTBENCH") {
        let mb: usize = mb.parse().unwrap();
        let threads: usize = std::env::var("FB_THREADS").ok().and_then(|x| x.parse().ok()).unwrap_or(1);
        for round in 0..3 {
            let t = Instant::now();
            std::thread::scope(|s| {
                for _ in 0..threads {
                    s.spawn(|| {
                        let mut v: Vec<u8> = Vec::with_capacity((mb << 20) / threads);
                        unsafe { v.set_len((mb << 20) / threads) };
                        for i in (0..v.len()).step_by(4096) { v[i] = 1; }
                        std::hint::black_box(&v);
                    });
                }
            });
            eprintln!("round {round} threads {threads}: {:?} = {:.2} us/fault (wall)", t.elapsed(), t.elapsed().as_secs_f64() * 1e6 / ((mb << 20) / 4096) as f64);
        }
        let t = Instant::now();
        let mut v: Vec<u8> = Vec::with_capacity(mb << 20);
        unsafe { v.set_len(mb << 20) };
        for i in (0..v.len()).step_by(4096) { v[i] = 1; }
        let a = t.elapsed();
        let t = Instant::now();
        let mut s = 0u64;
        for _ in 0..4 { for i in (0..v.len()).step_by(64) { s = s.wrapping_add(v[i] as u64); } }
        eprintln!("fault-touch {}MB: {:?} ({:.2} us/fault); 4x read pass {:?} {} {}", mb, a, a.as_secs_f64()*1e6/((mb<<20)/4096) as f64, t.elapsed(), s, mem_stats());
        return;
    }
    if let Ok(dir) = std::env::var("PARSEBENCH") {
        let files: Vec<S> = list_csv(&Path::new(&dir).join("events"))
            .iter()
            .map(|p| &*Box::leak(std::fs::read(p).unwrap().into_boxed_slice()))
            .collect();
        let chunks = make_chunks(&files, &["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]);
        let bytes: usize = files.iter().map(|f| f.len()).sum();
        for round in 0..3 {
            let t = Instant::now();
            let mut n = 0usize;
            for c in &chunks { scan_rows::<7>(c.data, |f| n += f[0].len()); }
            let a = t.elapsed();
            let t = Instant::now();
            let mut h = 0u64;
            for c in &chunks { scan_rows::<7>(c.data, |f| { h ^= key(f[0]).0 ^ key(f[1]).0; }); }
            let b = t.elapsed();
            let t = Instant::now();
            let mut z = 0i64;
            for c in &chunks { scan_rows::<7>(c.data, |f| { z += parse_ts(f[2]).unwrap_or(0) + parse_ts(f[3]).unwrap_or(0); }); }
            let c2 = t.elapsed();
            let ctx = Ctx { ws: 0, we: i64::MAX, products: HashMap::new(), campaigns: HashMap::new(), unknown_cat: 0, ncamp: 0, day0: 0, ndays: 1 };
            let t = Instant::now();
            let mut m = 0usize;
            for c in &chunks { m += parse_events(c, &ctx).iter().map(|v| v.len()).sum::<usize>(); }
            let d = t.elapsed();
            eprintln!("round {round}: {} MB, {} rows | scan {:?} ({:.0} MB/s) | scan+2 hashes {:?} | scan+2 ts {:?} | parse_events {:?} ({:.0} ns/row) {} {}",
                bytes >> 20, m, a, bytes as f64 / a.as_secs_f64() / 1e6, b, c2, d, d.as_nanos() as f64 / m as f64, n, h as u8 as i64 + z % 2);
        }
        return;
    }
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let (data, out) = if args.len() >= 3 {
        (PathBuf::from(&args[args.len() - 2]), PathBuf::from(&args[args.len() - 1]))
    } else {
        (PathBuf::from(env("DATA_DIR", "/data")), PathBuf::from(env("OUTPUT_DIR", "/output")))
    };
    let threads: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));
    rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().unwrap();

    let manifest = std::fs::read_to_string(data.join("manifest.json")).expect("read manifest.json");
    let ws = manifest_ts(&manifest, "window_start");
    let we = manifest_ts(&manifest, "window_end");
    log!(t0, "window {ws}..{we}, {threads} threads");

    // Map every input file (readahead starts immediately).
    let ev_paths = list_csv(&data.join("events"));
    let ck_paths = list_csv(&data.join("clicks"));
    let ln_paths = list_csv(&data.join("links"));
    let ev_files: Vec<S> = ev_paths.par_iter().map(|p| map_file(p)).collect();
    let ck_files: Vec<S> = ck_paths.par_iter().map(|p| map_file(p)).collect();
    let ln_files: Vec<S> = ln_paths.par_iter().map(|p| map_file(p)).collect();

    // Dimension tables.
    let mut cat_names: Vec<S> = Vec::new();
    let mut cat_idx: HashMap<S, u16> = HashMap::new();
    let mut intern_cat = |s: S, names: &mut Vec<S>| -> u16 {
        *cat_idx.entry(s).or_insert_with(|| {
            names.push(s);
            assert!(names.len() < u16::MAX as usize, "too many categories");
            (names.len() - 1) as u16
        })
    };
    let mut products: HashMap<S, u16> = HashMap::new();
    for [p, c] in read_small_table(&data.join("products.csv"), &["product_id", "category"]) {
        let ci = intern_cat(c, &mut cat_names);
        products.insert(p, ci);
    }
    let unknown_cat = intern_cat(b"unknown", &mut cat_names);
    let mut order: Vec<u32> = (0..cat_names.len() as u32).collect();
    order.sort_by(|&a, &b| cat_names[a as usize].cmp(cat_names[b as usize]));
    let mut rank = vec![0u32; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        rank[c as usize] = r as u32;
    }
    let mut campaigns: HashMap<S, u32> = HashMap::new();
    let mut camp_rows: Vec<[S; 2]> = Vec::new();
    for [id, ch] in read_small_table(&data.join("campaigns.csv"), &["campaign_id", "channel"]) {
        if !campaigns.contains_key(id) {
            campaigns.insert(id, camp_rows.len() as u32);
            camp_rows.push([id, ch]);
        }
    }
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1).max(1) as usize;
    let ctx = Ctx { ws, we, products, campaigns, unknown_cat, ncamp: camp_rows.len() as u32, day0, ndays };
    log!(t0, "dims: {} products, {} categories, {} campaigns", ctx.products.len(), cat_names.len(), ctx.ncamp);

    let ev_chunks = make_chunks(
        &ev_files,
        &["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"],
    );
    let ck_chunks = make_chunks(&ck_files, &["click_id", "user_id", "campaign_id", "ts"]);

    // Links and event parsing are independent; dedup and clicks then need links.
    let (links, ev_parsed) = rayon::join(
        || {
            let t = Instant::now();
            let l = build_links(&ln_files);
            prof(0, t);
            l
        },
        || ev_chunks.par_iter().map(|c| parse_events(c, &ctx)).collect::<Vec<_>>(),
    );
    log!(t0, "links built, events parsed ({} chunks)", ev_chunks.len());
    let (ev_by_person, ck_by_person) = rayon::join(
        || transpose(ev_parsed).into_par_iter().map(|p| dedup_events(p, &links)).collect::<Vec<_>>(),
        || ck_chunks.par_iter().map(|c| parse_clicks(c, &ctx, &links)).collect::<Vec<_>>(),
    );
    let (re, rc) = (ev_by_person, ck_by_person);
    log!(t0, "events deduped, clicks parsed");

    let (users, camp) = transpose(re)
        .into_par_iter()
        .zip(transpose(rc).into_par_iter())
        .fold(
            || (Vec::new(), CampAcc::new(&ctx)),
            |(mut users, mut acc), (evs, cks)| {
                users.push(process_partition(evs, cks, &ctx, &rank, &mut acc));
                (users, acc)
            },
        )
        .reduce(
            || (Vec::new(), CampAcc::new(&ctx)),
            |(mut u1, a1), (u2, a2)| {
                u1.extend(u2);
                (u1, a1.merge(a2))
            },
        );
    let nusers: usize = users.iter().map(|v| v.len()).sum();
    log!(t0, "sessionized: {nusers} user_daily rows");

    let organic: S = b"organic";
    let mut crow = Vec::new();
    for c in 0..=ctx.ncamp as usize {
        for d in 0..ndays {
            let a = camp.dense[c * ndays + d];
            if a[0] > 0 || a[3] > 0 {
                let (id, ch) = if c == ctx.ncamp as usize { (organic, organic) } else { (camp_rows[c][0], camp_rows[c][1]) };
                crow.push(output::CRow {
                    id,
                    channel: ch,
                    day: (day0 + d as i64) as i32,
                    sessions: a[0],
                    purchasing: a[1],
                    revenue: a[2],
                    assisted: a[3],
                });
            }
        }
    }
    output::write_all(&out, users, crow, &cat_names, threads);
    log!(t0, "done");
    eprintln!("cpu seconds: {}", prof_report());
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows<const N: usize>(s: &'static [u8]) -> Vec<Vec<&'static [u8]>> {
        let mut v = Vec::new();
        scan_rows::<N>(s, |f| v.push(f.to_vec()));
        v
    }

    #[test]
    fn scanner_matches_dialect() {
        let r = rows::<3>(b"a,\"b\",\n\"\",x,y\na,b\na,b,c,d\n\"a,b,c\na,b,c\"\na,b\"x\",c\na,\",c\n\n a , b ,c \nlast,no,nl");
        assert_eq!(
            r,
            vec![
                vec![&b"a"[..], b"b", b""],
                vec![&b""[..], b"x", b"y"],
                vec![&b" a "[..], b" b ", b"c "],
                vec![&b"last"[..], b"no", b"nl"],
            ]
        );
        assert_eq!(rows::<2>(b"x,y\r\n"), vec![vec![&b"x"[..], b"y\r"]]);
        assert_eq!(rows::<2>(b"\"x\",\"y\"\r\n"), Vec::<Vec<&[u8]>>::new());
        assert_eq!(rows::<2>(b"\"\"\"\",y\n"), Vec::<Vec<&[u8]>>::new());
        assert_eq!(rows::<2>(b"\",y\n"), Vec::<Vec<&[u8]>>::new());
    }
}
