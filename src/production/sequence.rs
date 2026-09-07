//! Offline natural-run repair. Only native channel sequence is a sort key.
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::ProductionError;

const HEADER_BYTES: u64 = 5;
const FAN_IN: usize = 32;

/// An adjacent inversion in the original input, before target filtering.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SequenceRegression {
    pub channel: u32,
    pub stream: String,
    pub previous_sequence: u64,
    pub sequence: u64,
    pub previous_source_row: u64,
    pub source_row: u64,
}

/// Repair of selected rows in one channel and one input stream.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SequenceRepair {
    pub channel: u32,
    pub stream: String,
    pub rows: u64,
    pub natural_runs: usize,
    pub merge_passes: usize,
}

#[derive(Default)]
pub(crate) struct NaturalRuns {
    last: Option<u64>,
    rows: u64,
    starts: Vec<u64>,
}

impl NaturalRuns {
    pub(crate) fn observe(&mut self, channel: u32, sequence: u64) -> Result<(), ProductionError> {
        if self.last == Some(sequence) {
            return Err(ProductionError::AmbiguousSequence { channel, sequence });
        }
        if self.last.is_none_or(|previous| sequence < previous) {
            self.starts.push(self.rows);
        }
        self.last = Some(sequence);
        self.rows = self
            .rows
            .checked_add(1)
            .ok_or(ProductionError::Arithmetic("spool rows"))?;
        Ok(())
    }

    pub(crate) fn repair(
        &self,
        path: &Path,
        channel: u32,
        stream: &str,
        row_bytes: u64,
    ) -> Result<Option<(PathBuf, SequenceRepair)>, ProductionError> {
        if self.starts.len() <= 1 {
            return Ok(None);
        }
        let mut runs: Vec<_> = self
            .starts
            .iter()
            .enumerate()
            .map(|(i, &start)| Run {
                start,
                rows: self.starts.get(i + 1).copied().unwrap_or(self.rows) - start,
            })
            .collect();
        let mut input = path.to_path_buf();
        let mut passes = 0;
        while runs.len() > 1 {
            passes += 1;
            let output = path.with_extension(format!("repaired-{passes}.bin"));
            runs = merge_pass(&input, &output, &runs, row_bytes, channel)?;
            input = output;
        }
        Ok(Some((
            input,
            SequenceRepair {
                channel,
                stream: stream.to_owned(),
                rows: self.rows,
                natural_runs: self.starts.len(),
                merge_passes: passes,
            },
        )))
    }
}

struct Run {
    start: u64,
    rows: u64,
}

struct Cursor {
    reader: BufReader<Take<File>>,
    remaining: u64,
    bytes: Vec<u8>,
}

impl Cursor {
    fn open(path: &Path, run: &Run, row_bytes: u64) -> Result<Self, ProductionError> {
        let offset = run
            .start
            .checked_mul(row_bytes)
            .and_then(|v| v.checked_add(HEADER_BYTES))
            .ok_or(ProductionError::Arithmetic("run offset"))?;
        let length = run
            .rows
            .checked_mul(row_bytes)
            .ok_or(ProductionError::Arithmetic("run length"))?;
        // Separate open, not try_clone: cursors must not share a file offset.
        let mut file = File::open(path).map_err(|e| ProductionError::io(path, e))?;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| ProductionError::io(path, e))?;
        Ok(Self {
            reader: BufReader::with_capacity(256 * 1024, file.take(length)),
            remaining: run.rows,
            bytes: vec![0; row_bytes as usize],
        })
    }

    fn advance(&mut self, path: &Path) -> Result<Option<u64>, ProductionError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.reader
            .read_exact(&mut self.bytes)
            .map_err(|e| ProductionError::io(path, e))?;
        self.remaining -= 1;
        let mut sequence = [0; 8];
        sequence.copy_from_slice(&self.bytes[8..16]);
        Ok(Some(u64::from_le_bytes(sequence)))
    }
}

fn merge_pass(
    input: &Path,
    output: &Path,
    runs: &[Run],
    row_bytes: u64,
    channel: u32,
) -> Result<Vec<Run>, ProductionError> {
    let mut header = [0; HEADER_BYTES as usize];
    File::open(input)
        .and_then(|mut f| f.read_exact(&mut header))
        .map_err(|e| ProductionError::io(input, e))?;
    let mut writer = BufWriter::with_capacity(
        256 * 1024,
        File::create(output).map_err(|e| ProductionError::io(output, e))?,
    );
    writer
        .write_all(&header)
        .map_err(|e| ProductionError::io(output, e))?;
    let mut result = Vec::new();
    let mut total = 0_u64;
    for group in runs.chunks(FAN_IN) {
        let start = total;
        let mut cursors = group
            .iter()
            .map(|r| Cursor::open(input, r, row_bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let mut heap = BinaryHeap::new();
        for (index, cursor) in cursors.iter_mut().enumerate() {
            if let Some(sequence) = cursor.advance(input)? {
                heap.push(Reverse((sequence, index)));
            }
        }
        let mut previous = None;
        while let Some(Reverse((sequence, index))) = heap.pop() {
            // Run index never resolves a duplicate: equality is always an error.
            if previous.is_some_and(|p| sequence <= p) {
                return Err(ProductionError::AmbiguousSequence { channel, sequence });
            }
            previous = Some(sequence);
            writer
                .write_all(&cursors[index].bytes)
                .map_err(|e| ProductionError::io(output, e))?;
            total = total
                .checked_add(1)
                .ok_or(ProductionError::Arithmetic("merged rows"))?;
            if let Some(next) = cursors[index].advance(input)? {
                heap.push(Reverse((next, index)));
            }
        }
        result.push(Run {
            start,
            rows: total - start,
        });
    }
    writer.flush().map_err(|e| ProductionError::io(output, e))?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    fn check(sequences: &[u64], row_bytes: u64) -> Result<(Vec<u64>, usize), ProductionError> {
        let dir = tempfile::tempdir().expect("temp");
        let path = dir.path().join("channel-2015.orders.bin");
        let mut bytes = b"QTP1\x02".to_vec();
        let mut runs = NaturalRuns::default();
        for (i, &sequence) in sequences.iter().enumerate() {
            runs.observe(2015, sequence)?;
            let mut row = vec![0xA5; row_bytes as usize];
            row[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            row[8..16].copy_from_slice(&sequence.to_le_bytes());
            bytes.extend(row);
        }
        std::fs::write(&path, &bytes).expect("spool");
        let repaired = runs.repair(&path, 2015, "orders", row_bytes)?;
        let passes = repaired.as_ref().map_or(0, |(_, r)| r.merge_passes);
        let output = repaired.map_or_else(|| path.clone(), |(p, _)| p);
        let result = std::fs::read(output).expect("read");
        assert_eq!(std::fs::read(&path).expect("original"), bytes);
        assert_eq!(result.len(), bytes.len());
        let mut native = Vec::new();
        for row in result[5..].chunks_exact(row_bytes as usize) {
            let source = u64::from_le_bytes(row[..8].try_into().expect("source"));
            let original_offset = 5 + (source as usize - 1) * row_bytes as usize;
            assert_eq!(
                row,
                &bytes[original_offset..original_offset + row_bytes as usize]
            );
            native.push(u64::from_le_bytes(row[8..16].try_into().expect("sequence")));
        }
        Ok((native, passes))
    }

    #[test]
    fn normal_empty_singleton_and_sparse_streams_do_not_rewrite() {
        for seq in [vec![], vec![7], vec![1, 50, 900]] {
            assert_eq!(check(&seq, 60).expect("normal"), (seq, 0));
        }
    }

    #[test]
    fn merges_long_inversion_and_preserves_every_byte() {
        for size in [60, 75] {
            assert_eq!(
                check(&[1, 3, 1000, 2, 4, 1001], size).expect("repair"),
                (vec![1, 2, 3, 4, 1000, 1001], 1)
            );
        }
    }

    #[test]
    fn bounded_fan_in_supports_multiple_passes() {
        let seq: Vec<u64> = (1..=1100).rev().collect();
        let (result, passes) = check(&seq, 60).expect("repair");
        assert_eq!(result, (1..=1100).collect::<Vec<_>>());
        assert_eq!(passes, 3);
    }

    #[test]
    fn duplicate_within_or_across_runs_is_never_deduplicated() {
        for seq in [
            vec![1, 1],
            vec![1, 3, 2, 3],
            (1..=40).rev().chain([40]).collect(),
        ] {
            assert!(matches!(
                check(&seq, 60),
                Err(ProductionError::AmbiguousSequence { .. })
            ));
        }
    }

    proptest::proptest! {
        #[test]
        fn arbitrary_unique_runs_equal_reference_sort(seq in proptest::collection::vec(1_u64..10_000, 0..180)) {
            let unique: std::collections::HashSet<_> = seq.iter().collect();
            if unique.len() == seq.len() {
                let mut sorted = seq.clone();
                sorted.sort_unstable();
                proptest::prop_assert_eq!(check(&seq, 75).expect("repair").0, sorted);
            } else {
                proptest::prop_assert!(check(&seq, 75).is_err());
            }
        }
    }
}
