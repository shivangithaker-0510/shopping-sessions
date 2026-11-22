//! Shopping Sessions pipeline: clean -> link IDs -> sessionize -> attribute ->
//! assists -> two daily Parquet tables. See rules.md.

mod output;
mod parse;

use hashbrown::hash_table::Entry;
use hashbrown::{HashMap, HashTable};
use memchr::memchr;
use parse::*;
use rayon::prelude::*;
use std::path::{Path, PathBuf};
use std::time::Instant;
use xxhash_rust::xxh3::xxh3_64;

/// Partition count (power of two) for hash-partitioned stages.
const PBITS: u32 = 8;
const NPART: usize = 1 << PBITS;
/// Target size of one parse task.
const CHUNK: usize = 8 << 20;

#[inline]
fn part_of(h: u64) -> usize {
    // Middle bits: hashbrown uses the low bits for slots and the top 7 for tags.
    ((h >> 40) as usize) & (NPART - 1)
}

#[inline]
fn hash(s: &[u8]) -> u64 {
    xxh3_64(s)
}

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {
        eprintln!("[{:>8.3}s] {} {}", $t0.elapsed().as_secs_f64(), format!($($arg)*), mem_stats())
    };
}

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
static PROF: [AtomicU64; 8] = [const { AtomicU64::new(0) }; 8];
const PROF_NAMES: [&str; 8] = ["parse_ev", "dedup_tab", "dedup_route", "parse_ck", "proc_group", "proc_sort", "proc_person", "links"];
#[inline]
fn prof(i: usize, t: Instant) {
    PROF[i].fetch_add(t.elapsed().as_nanos() as u64, Relaxed);
}
fn prof_report() -> String {
    (0..8).map(|i| format!("{}={:.2}", PROF_NAMES[i], PROF[i].load(Relaxed) as f64 / 1e9)).collect::<Vec<_>>().join(" ")
}

/// Peak RSS and page-fault counts (Linux only; empty elsewhere).
fn mem_stats() -> String {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let hwm = status.lines().find(|l| l.starts_with("VmHWM")).unwrap_or("").replace(' ', "");
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    if f.len() > 10 {
        format!("| {hwm} minflt={} majflt={}", f[7], f[9])
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
    let m = unsafe { memmap2::Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", p.display()));
    #[cfg(unix)]
    {
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

/// Call `f` with each line (without its `\n`) of a chunk.
#[inline]
fn for_lines(data: S, mut f: impl FnMut(S)) {
    let len = data.len();
    let mut pos = 0;
    while pos < len {
        let e = match memchr(b'\n', &data[pos..]) {
            Some(p) => pos + p,
            None => len,
        };
        f(&data[pos..e]);
        pos = e + 1;
    }
}

fn read_small_table(path: &Path, wanted: &[&str]) -> Vec<[S; 2]> {
    let f = if path.is_file() { map_file(path) } else { &[] };
    let chunks = make_chunks(&[f], wanted);
    let mut rows = Vec::new();
    for c in &chunks {
        for_lines(c.data, |line| {
            let mut fl = [&b""[..]; 2];
            if split_line(line, &mut fl) {
                rows.push([fl[c.map[0] as usize], fl[c.map[1] as usize]]);
            }
        });
    }
    rows
}

// ---------------------------------------------------------------- links

/// IDs that appear in links, interned per hash partition, each mapped to its
/// person's name (the byte-wise smallest ID in its component).
struct Links {
    tables: Vec<HashTable<u32>>,
    keys: Vec<Vec<(S, u64)>>,
    offs: Vec<u32>,
    name: Vec<(S, u64)>,
}

impl Links {
    #[inline]
    fn person(&self, id: S, h: u64) -> (S, u64) {
        let b = part_of(h);
        let keys = &self.keys[b];
        match self.tables[b].find(h, |&i| keys[i as usize].0 == id) {
            Some(&i) => self.name[(self.offs[b] + i) as usize],
            None => (id, h),
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
    // Parse, and scatter both sides by hash partition. Item: (hash, side key, id).
    let per_task: Vec<Vec<(S, S)>> = chunks
        .par_iter()
        .map(|c| {
            let mut pairs = Vec::new();
            for_lines(c.data, |line| {
                let mut f = [&b""[..]; 2];
                if !split_line(line, &mut f) {
                    return;
                }
                let a = f[c.map[0] as usize];
                let b = f[c.map[1] as usize];
                if a.is_empty() || b.is_empty() {
                    return;
                }
                pairs.push((a, b));
            });
            pairs
        })
        .collect();
    let mut base = Vec::with_capacity(per_task.len() + 1);
    base.push(0u32);
    for p in &per_task {
        base.push(base.last().unwrap() + p.len() as u32);
    }
    let npairs = *base.last().unwrap() as usize;
    let scattered: Vec<Vec<Vec<(u64, u32, S)>>> = per_task
        .par_iter()
        .enumerate()
        .map(|(t, pairs)| {
            let mut out: Vec<Vec<(u64, u32, S)>> = (0..NPART).map(|_| Vec::new()).collect();
            for (j, &(a, b)) in pairs.iter().enumerate() {
                let k = (base[t] + j as u32) * 2;
                let ha = hash(a);
                out[part_of(ha)].push((ha, k, a));
                let hb = hash(b);
                out[part_of(hb)].push((hb, k + 1, b));
            }
            out
        })
        .collect();
    let mut by_part: Vec<Vec<Vec<(u64, u32, S)>>> = (0..NPART).map(|_| Vec::new()).collect();
    for t in scattered {
        for (p, v) in t.into_iter().enumerate() {
            by_part[p].push(v);
        }
    }
    // Intern each partition.
    let interned: Vec<(HashTable<u32>, Vec<(S, u64)>, Vec<(u32, u32)>)> = by_part
        .into_par_iter()
        .map(|vs| {
            let n: usize = vs.iter().map(|v| v.len()).sum();
            let mut table: HashTable<u32> = HashTable::with_capacity(n);
            let mut keys: Vec<(S, u64)> = Vec::with_capacity(n);
            let mut assign = Vec::with_capacity(n);
            for v in vs {
                for (h, k, s) in v {
                    let id = match table.entry(h, |&i| keys[i as usize].0 == s, |&i| keys[i as usize].1) {
                        Entry::Occupied(o) => *o.get(),
                        Entry::Vacant(vac) => {
                            let id = keys.len() as u32;
                            vac.insert(id);
                            keys.push((s, h));
                            id
                        }
                    };
                    assign.push((k, id));
                }
            }
            (table, keys, assign)
        })
        .collect();
    let mut offs = Vec::with_capacity(NPART + 1);
    offs.push(0u32);
    for (_, k, _) in &interned {
        offs.push(offs.last().unwrap() + k.len() as u32);
    }
    let nids = *offs.last().unwrap() as usize;
    let mut side = vec![0u32; npairs * 2];
    for (b, (_, _, assign)) in interned.iter().enumerate() {
        for &(k, id) in assign {
            side[k as usize] = offs[b] + id;
        }
    }
    let mut all_keys: Vec<S> = Vec::with_capacity(nids);
    for (_, k, _) in &interned {
        all_keys.extend(k.iter().map(|x| x.0));
    }
    // Union-find.
    let mut parent: Vec<u32> = (0..nids as u32).collect();
    for p in 0..npairs {
        let a = find(&mut parent, side[2 * p]);
        let b = find(&mut parent, side[2 * p + 1]);
        if a != b {
            // Keep the root whose own key is smaller (cheap; names are fixed below anyway).
            if all_keys[a as usize] <= all_keys[b as usize] {
                parent[b as usize] = a;
            } else {
                parent[a as usize] = b;
            }
        }
    }
    // The smallest ID of each component.
    let mut best: Vec<u32> = (0..nids as u32).collect();
    for i in 0..nids as u32 {
        let r = find(&mut parent, i) as usize;
        if all_keys[i as usize] < all_keys[best[r] as usize] {
            best[r] = i;
        }
    }
    let mut name_hash: Vec<u64> = vec![0; nids];
    for (b, (_, k, _)) in interned.iter().enumerate() {
        for (i, x) in k.iter().enumerate() {
            name_hash[offs[b] as usize + i] = x.1;
        }
    }
    let mut name = Vec::with_capacity(nids);
    for i in 0..nids as u32 {
        let r = find(&mut parent, i) as usize;
        let n = best[r] as usize;
        name.push((all_keys[n], name_hash[n]));
    }
    let (tables, keys): (Vec<_>, Vec<_>) = interned.into_iter().map(|(t, k, _)| (t, k)).unzip();
    Links { tables, keys, offs, name }
}

// ---------------------------------------------------------------- events & clicks

const K_VIEW: u8 = 1;
const K_CART: u8 = 2;
const K_PURCHASE: u8 = 3;

/// One parsed copy of an event. kind == 0 means the copy fails validation.
#[derive(Clone, Copy)]
struct Ev {
    eid: S,
    user: S,
    eh: u64,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u32,
    kind: u8,
}

/// A surviving event, keyed by its person.
#[derive(Clone, Copy)]
struct PEv {
    name: S,
    nh: u64,
    ts: i64,
    price: u32,
    cat: u32,
    kind: u8,
}

/// A valid click, keyed by its person.
#[derive(Clone, Copy)]
struct PCk {
    name: S,
    nh: u64,
    ts: i64,
    cid: S,
    camp: u32,
}

struct Ctx {
    ws: i64,
    we: i64,
    products: HashMap<S, u32>,
    campaigns: HashMap<S, u32>,
    unknown_cat: u32,
    ncamp: u32,
    day0: i64,
    ndays: usize,
}

fn parse_events(c: &Chunk, ctx: &Ctx) -> Vec<Vec<Ev>> {
    let _t = Instant::now();
    let est = c.data.len() / 70 / NPART + 8;
    let mut out: Vec<Vec<Ev>> = (0..NPART).map(|_| Vec::with_capacity(est)).collect();
    let m = c.map;
    for_lines(c.data, |line| {
        let mut f = [&b""[..]; 7];
        if !split_line(line, &mut f) {
            return;
        }
        let eid = f[m[0] as usize];
        if eid.is_empty() {
            return;
        }
        let user = f[m[1] as usize];
        let ts_raw = f[m[2] as usize];
        let ingest_raw = f[m[3] as usize];
        let et = f[m[4] as usize];
        let product = f[m[5] as usize];
        let price_raw = f[m[6] as usize];
        let (ingest, ingest_ok) = match parse_ts(ingest_raw) {
            Some(x) => (x, true),
            None => (i64::MAX, false),
        };
        let mut kind = match et {
            b"view" => K_VIEW,
            b"cart" => K_CART,
            b"purchase" => K_PURCHASE,
            _ => 0,
        };
        let mut ts = 0;
        let mut price = 0;
        let mut cat = 0;
        if kind != 0 {
            if !ingest_ok || user.is_empty() || product.is_empty() {
                kind = 0;
            } else {
                match parse_ts(ts_raw) {
                    Some(t) if t >= ctx.ws && t < ctx.we && (ingest as i128 - t as i128) <= DAY as i128 => {
                        ts = t
                    }
                    _ => kind = 0,
                }
            }
            if kind == K_PURCHASE {
                match parse_price(price_raw) {
                    Some(p) => {
                        price = p;
                        cat = *ctx.products.get(product).unwrap_or(&ctx.unknown_cat);
                    }
                    None => kind = 0,
                }
            }
        }
        let eh = hash(eid);
        out[part_of(eh)].push(Ev { eid, user, eh, ingest, ts, price, cat, kind });
    });
    prof(0, _t);
    out
}

/// Dedup one event-id partition (smallest ingest_ts wins, before validation),
/// then route survivors to person partitions.
fn dedup_events(parts: Vec<Vec<Ev>>, links: &Links) -> Vec<Vec<PEv>> {
    let _t = Instant::now();
    let n: usize = parts.iter().map(|v| v.len()).sum();
    let mut best: Vec<Ev> = Vec::with_capacity(n);
    let mut table: HashTable<u32> = HashTable::with_capacity(n);
    for v in parts {
        for e in v {
            match table.entry(e.eh, |&i| best[i as usize].eid == e.eid, |&i| best[i as usize].eh) {
                Entry::Occupied(o) => {
                    let i = *o.get() as usize;
                    if e.ingest < best[i].ingest {
                        best[i] = e;
                    }
                }
                Entry::Vacant(vac) => {
                    vac.insert(best.len() as u32);
                    best.push(e);
                }
            }
        }
    }
    drop(table);
    prof(1, _t);
    let _t = Instant::now();
    let est = n / NPART + 8;
    let mut out: Vec<Vec<PEv>> = (0..NPART).map(|_| Vec::with_capacity(est)).collect();
    for e in best {
        if e.kind == 0 {
            continue;
        }
        let (name, nh) = links.person(e.user, hash(e.user));
        out[part_of(nh)].push(PEv { name, nh, ts: e.ts, price: e.price, cat: e.cat, kind: e.kind });
    }
    prof(2, _t);
    out
}

/// Parse and validate clicks. Duplicate click copies carry identical values,
/// so keeping them changes neither the credited click nor the assists.
fn parse_clicks(c: &Chunk, ctx: &Ctx, links: &Links) -> Vec<Vec<PCk>> {
    let _t = Instant::now();
    let mut out: Vec<Vec<PCk>> = (0..NPART).map(|_| Vec::new()).collect();
    let m = c.map;
    let lo = ctx.ws - WEEK;
    for_lines(c.data, |line| {
        let mut f = [&b""[..]; 4];
        if !split_line(line, &mut f) {
            return;
        }
        let cid = f[m[0] as usize];
        let user = f[m[1] as usize];
        if cid.is_empty() || user.is_empty() {
            return;
        }
        let ts = match parse_ts(f[m[3] as usize]) {
            Some(t) if t >= lo && t < ctx.we => t,
            _ => return,
        };
        let camp = match ctx.campaigns.get(f[m[2] as usize]) {
            Some(&c) => c,
            None => return,
        };
        let (name, nh) = links.person(user, hash(user));
        out[part_of(nh)].push(PCk { name, nh, ts, cid, camp });
    });
    prof(3, _t);
    out
}

// ---------------------------------------------------------------- sessions

#[derive(Clone, Copy, Default)]
struct E2 {
    ts: i64,
    price: u32,
    cat: u32,
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

struct Scratch {
    starts: Vec<i64>,
    dayis: Vec<usize>,
    cats: Vec<(u32, i64)>,
    by_camp: Vec<(u32, i64)>,
}

fn flush_day(name: S, day: i32, ur: &mut URow, cats: &mut Vec<(u32, i64)>, rank: &[u32], rows: &mut Vec<URow>) {
    let mut top = u32::MAX;
    let mut top_sum = i64::MIN;
    for &(c, s) in cats.iter() {
        if top == u32::MAX || s > top_sum || (s == top_sum && rank[c as usize] < rank[top as usize]) {
            top = c;
            top_sum = s;
        }
    }
    rows.push(URow { name, day, sessions: ur.sessions, events: ur.events, revenue: ur.revenue, top });
    cats.clear();
    ur.sessions = 0;
    ur.events = 0;
    ur.revenue = 0;
}

fn person(
    name: S,
    es: &[E2],
    cs: &[C2],
    ctx: &Ctx,
    rank: &[u32],
    acc: &mut CampAcc,
    rows: &mut Vec<URow>,
    sc: &mut Scratch,
) {
    let organic = ctx.ncamp;
    sc.starts.clear();
    sc.dayis.clear();
    sc.cats.clear();
    let mut ur = URow { name, day: 0, sessions: 0, events: 0, revenue: 0, top: 0 };
    let mut cur_day: i64 = i64::MIN;
    let mut i = 0;
    let n = es.len();
    while i < n {
        let start = es[i].ts;
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush_day(name, cur_day as i32, &mut ur, &mut sc.cats, rank, rows);
            }
            cur_day = day;
        }
        let mut last = start;
        let mut nev = 0i64;
        let mut rev = 0i64;
        let mut purch = false;
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
        ur.sessions += 1;
        ur.events += nev;
        ur.revenue += rev;
        // Credit: latest click at or before start, within 7 days; tie -> smallest click_id.
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
        flush_day(name, cur_day as i32, &mut ur, &mut sc.cats, rank, rows);
    }
    // Assists: a campaign assists a session when one of its clicks lies in
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

fn process_partition(
    evs: Vec<Vec<PEv>>,
    cks: Vec<Vec<PCk>>,
    ctx: &Ctx,
    rank: &[u32],
    acc: &mut CampAcc,
) -> Vec<URow> {
    let _t = Instant::now();
    let n: usize = evs.iter().map(|v| v.len()).sum();
    let mut rows = Vec::new();
    if n == 0 {
        return rows;
    }
    let mut names: Vec<(S, u64)> = Vec::new();
    let mut cnt: Vec<u32> = Vec::new();
    let mut table: HashTable<u32> = HashTable::with_capacity(n / 4 + 16);
    let mut pid: Vec<u32> = Vec::with_capacity(n);
    for v in &evs {
        for e in v {
            let id = match table.entry(e.nh, |&i| names[i as usize].0 == e.name, |&i| names[i as usize].1) {
                Entry::Occupied(o) => *o.get(),
                Entry::Vacant(vac) => {
                    let id = names.len() as u32;
                    vac.insert(id);
                    names.push((e.name, e.nh));
                    cnt.push(0);
                    id
                }
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
    let mut pos = off.clone();
    let mut arr = vec![E2::default(); n];
    let mut k = 0;
    for v in &evs {
        for e in v {
            let id = pid[k] as usize;
            k += 1;
            arr[pos[id] as usize] = E2 { ts: e.ts, price: e.price, cat: e.cat, kind: e.kind };
            pos[id] += 1;
        }
    }
    drop(evs);
    drop(pid);
    // Clicks of people who have events (others can credit nothing).
    let mut ccnt = vec![0u32; np + 1];
    let mut cpid: Vec<u32> = Vec::new();
    for v in &cks {
        for c in v {
            let id = match table.find(c.nh, |&i| names[i as usize].0 == c.name) {
                Some(&i) => {
                    ccnt[i as usize] += 1;
                    i
                }
                None => u32::MAX,
            };
            cpid.push(id);
        }
    }
    let mut coff = vec![0u32; np + 1];
    for p in 0..np {
        coff[p + 1] = coff[p] + ccnt[p];
    }
    let mut cpos = coff.clone();
    let nc = coff[np] as usize;
    let mut carr: Vec<C2> = vec![C2 { ts: 0, cid: &[], camp: 0 }; nc];
    let mut k = 0;
    for v in &cks {
        for c in v {
            let id = cpid[k];
            k += 1;
            if id != u32::MAX {
                carr[cpos[id as usize] as usize] = C2 { ts: c.ts, cid: c.cid, camp: c.camp };
                cpos[id as usize] += 1;
            }
        }
    }
    drop(cks);
    prof(4, _t);
    let mut sc = Scratch { starts: Vec::new(), dayis: Vec::new(), cats: Vec::new(), by_camp: Vec::new() };
    let (mut tsort, mut tperson) = (0u64, 0u64);
    for p in 0..np {
        let t1 = Instant::now();
        let es = &mut arr[off[p] as usize..off[p + 1] as usize];
        es.sort_unstable_by_key(|e| e.ts);
        let cs = &mut carr[coff[p] as usize..coff[p + 1] as usize];
        cs.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp(b.cid)));
        let t2 = Instant::now();
        person(names[p].0, es, cs, ctx, rank, acc, &mut rows, &mut sc);
        tsort += (t2 - t1).as_nanos() as u64;
        tperson += t2.elapsed().as_nanos() as u64;
    }
    PROF[5].fetch_add(tsort, Relaxed);
    PROF[6].fetch_add(tperson, Relaxed);
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

fn transpose<T>(v: Vec<Vec<Vec<T>>>) -> Vec<Vec<Vec<T>>> {
    let mut out: Vec<Vec<Vec<T>>> = (0..NPART).map(|_| Vec::with_capacity(v.len())).collect();
    for t in v {
        for (p, x) in t.into_iter().enumerate() {
            out[p].push(x);
        }
    }
    out
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
    let data = PathBuf::from(if args.len() >= 3 { args[args.len() - 2].clone() } else { env("DATA_DIR", "/data") });
    let out = PathBuf::from(if args.len() >= 3 { args[args.len() - 1].clone() } else { env("OUTPUT_DIR", "/output") });
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
    let mut cat_idx: HashMap<S, u32> = HashMap::new();
    let mut intern_cat = |s: S, names: &mut Vec<S>| -> u32 {
        *cat_idx.entry(s).or_insert_with(|| {
            names.push(s);
            (names.len() - 1) as u32
        })
    };
    let mut products: HashMap<S, u32> = HashMap::new();
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
    let ctx = Ctx {
        ws,
        we,
        products,
        campaigns,
        unknown_cat,
        ncamp: camp_rows.len() as u32,
        day0,
        ndays,
    };
    log!(t0, "dims: {} products, {} categories, {} campaigns", ctx.products.len(), cat_names.len(), ctx.ncamp);

    // Links and event parsing run together.
    let ev_chunks = make_chunks(&ev_files, &["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]);
    let (links, ev_parsed) = rayon::join(
        || { let _t = Instant::now(); let l = build_links(&ln_files); prof(7, _t); l },
        || ev_chunks.par_iter().map(|c| parse_events(c, &ctx)).collect::<Vec<_>>(),
    );
    log!(t0, "links: {} ids; events parsed in {} chunks", links.name.len(), ev_chunks.len());

    let ck_chunks = make_chunks(&ck_files, &["click_id", "user_id", "campaign_id", "ts"]);
    let (ev_by_person, ck_parsed) = rayon::join(
        || {
            transpose(ev_parsed)
                .into_par_iter()
                .map(|parts| dedup_events(parts, &links))
                .collect::<Vec<_>>()
        },
        || ck_chunks.par_iter().map(|c| parse_clicks(c, &ctx, &links)).collect::<Vec<_>>(),
    );
    log!(t0, "events deduped, clicks parsed");

    let ev_parts = transpose(ev_by_person);
    let ck_parts = transpose(ck_parsed);
    let (users, camp) = ev_parts
        .into_par_iter()
        .zip(ck_parts.into_par_iter())
        .fold(
            || (Vec::new(), CampAcc::new(&ctx)),
            |(mut users, mut acc), (evs, cks)| {
                let rows = process_partition(evs, cks, &ctx, &rank, &mut acc);
                users.push(rows);
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

    // Campaign rows.
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
