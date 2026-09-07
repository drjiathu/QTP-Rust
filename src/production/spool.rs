use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use super::ProductionError;
use super::sequence::{NaturalRuns, SequenceRegression, SequenceRepair};

const MAGIC: &[u8; 4] = b"QTP1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SseKind {
    Add,
    Delete,
    Trade,
    Status,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Aggressor {
    Buy,
    Sell,
    Neutral,
    Other,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SseRow {
    pub source_row: u64,
    pub sequence: u64,
    pub channel: u32,
    pub symbol: String,
    pub quote_time_ns: i64,
    pub local_time_ns: i64,
    pub kind: SseKind,
    pub buy_order_no: u64,
    pub sell_order_no: u64,
    pub price_units: i64,
    pub quantity: u64,
    pub flag: Aggressor,
    pub status: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SzOrderKind {
    Market,
    Limit,
    SameSideBest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SzSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SzOrderRow {
    pub source_row: u64,
    pub sequence: u64,
    pub channel: u32,
    pub symbol: String,
    pub quote_time_ns: i64,
    pub local_time_ns: i64,
    pub price_units: i64,
    pub quantity: u64,
    pub side: SzSide,
    pub kind: SzOrderKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SzExecutionKind {
    Trade,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SzExecutionRow {
    pub source_row: u64,
    pub sequence: u64,
    pub channel: u32,
    pub symbol: String,
    pub quote_time_ns: i64,
    pub local_time_ns: i64,
    pub bid_order_no: u64,
    pub ask_order_no: u64,
    pub price_units: i64,
    pub quantity: u64,
    pub kind: SzExecutionKind,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SpoolKind {
    Sse,
    SzOrder,
    SzExecution,
}

impl SpoolKind {
    const fn byte(self) -> u8 {
        match self {
            Self::Sse => 1,
            Self::SzOrder => 2,
            Self::SzExecution => 3,
        }
    }

    const fn suffix(self) -> &'static str {
        match self {
            Self::Sse => "sse",
            Self::SzOrder => "orders",
            Self::SzExecution => "executions",
        }
    }
}

pub(crate) struct SpoolSet {
    root: PathBuf,
    writers: HashMap<(u32, SpoolKind), BufWriter<File>>,
    runs: HashMap<(u32, SpoolKind), NaturalRuns>,
    input_sequences: HashMap<(u32, SpoolKind), (u64, u64)>,
    regressions: Vec<SequenceRegression>,
}

impl SpoolSet {
    pub(crate) fn create(temp_root: &Path, label: &str) -> Result<Self, ProductionError> {
        fs::create_dir_all(temp_root).map_err(|error| ProductionError::io(temp_root, error))?;
        let directory = tempfile::Builder::new()
            .prefix(&format!("qtp-replay-{label}-"))
            .tempdir_in(temp_root)
            .map_err(|error| ProductionError::io(temp_root, error))?;
        let root = directory.keep();
        Ok(Self {
            root,
            writers: HashMap::new(),
            runs: HashMap::new(),
            input_sequences: HashMap::new(),
            regressions: Vec::new(),
        })
    }

    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn write_sse(&mut self, row: &SseRow) -> Result<(), ProductionError> {
        let writer = self.writer(row.channel, SpoolKind::Sse)?;
        write_sse(writer, row).map_err(|error| ProductionError::io(&self.root, error))
    }

    pub(crate) fn write_sz_order(&mut self, row: &SzOrderRow) -> Result<(), ProductionError> {
        self.runs
            .entry((row.channel, SpoolKind::SzOrder))
            .or_default()
            .observe(row.channel, row.sequence)?;
        let writer = self.writer(row.channel, SpoolKind::SzOrder)?;
        write_sz_order(writer, row).map_err(|error| ProductionError::io(&self.root, error))
    }

    pub(crate) fn write_sz_execution(
        &mut self,
        row: &SzExecutionRow,
    ) -> Result<(), ProductionError> {
        self.runs
            .entry((row.channel, SpoolKind::SzExecution))
            .or_default()
            .observe(row.channel, row.sequence)?;
        let writer = self.writer(row.channel, SpoolKind::SzExecution)?;
        write_sz_execution(writer, row).map_err(|error| ProductionError::io(&self.root, error))
    }

    pub(crate) fn finish(mut self) -> Result<FinishedSpool, ProductionError> {
        let root = self.root.clone();
        self.finish_inner()
            .map_err(|source| ProductionError::ReplayFailed {
                spool_path: root,
                source: Box::new(source),
            })
    }

    fn finish_inner(&mut self) -> Result<FinishedSpool, ProductionError> {
        for writer in self.writers.values_mut() {
            writer
                .flush()
                .map_err(|error| ProductionError::io(&self.root, error))?;
        }
        self.writers.clear();
        let audit = self.root.join("sequence-regressions.json");
        let bytes = serde_json::to_vec_pretty(&self.regressions)
            .map_err(|e| ProductionError::InvalidRequest(e.to_string()))?;
        fs::write(&audit, bytes).map_err(|e| ProductionError::io(&audit, e))?;
        let mut repaired = HashMap::new();
        let mut repairs = Vec::new();
        let mut keys: Vec<_> = self.runs.keys().copied().collect();
        keys.sort_by_key(|(channel, kind)| (*channel, kind.byte()));
        for (channel, kind) in keys {
            // QTP1 common fields: 42 bytes; order adds 18, execution adds 33.
            let row_bytes = match kind {
                SpoolKind::SzOrder => 60,
                SpoolKind::SzExecution => 75,
                SpoolKind::Sse => unreachable!(),
            };
            if let Some((path, repair)) = self.runs[&(channel, kind)].repair(
                &spool_path(&self.root, channel, kind),
                channel,
                kind.suffix(),
                row_bytes,
            )? {
                repaired.insert((channel, kind), path);
                repairs.push(repair);
            }
        }
        Ok(FinishedSpool {
            root: self.root.clone(),
            repaired,
            repairs,
            regressions: self.regressions.clone(),
        })
    }

    /// Scan-wide audit, before universe/time filtering. Not an ordering key.
    pub(crate) fn observe_sz_sequence(
        &mut self,
        channel: u32,
        sequence: u64,
        source_row: u64,
        execution: bool,
    ) -> Result<(), ProductionError> {
        let kind = if execution {
            SpoolKind::SzExecution
        } else {
            SpoolKind::SzOrder
        };
        if let Some((previous, previous_row)) = self
            .input_sequences
            .insert((channel, kind), (sequence, source_row))
        {
            if sequence == previous {
                return Err(ProductionError::AmbiguousSequence { channel, sequence });
            }
            if sequence < previous {
                self.regressions.push(SequenceRegression {
                    channel,
                    stream: kind.suffix().to_owned(),
                    previous_sequence: previous,
                    sequence,
                    previous_source_row: previous_row,
                    source_row,
                });
            }
        }
        Ok(())
    }

    fn writer(
        &mut self,
        channel: u32,
        kind: SpoolKind,
    ) -> Result<&mut BufWriter<File>, ProductionError> {
        match self.writers.entry((channel, kind)) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let path = spool_path(&self.root, channel, kind);
                let mut writer = BufWriter::new(
                    File::create(&path).map_err(|error| ProductionError::io(&path, error))?,
                );
                writer
                    .write_all(MAGIC)
                    .and_then(|()| writer.write_all(&[kind.byte()]))
                    .map_err(|error| ProductionError::io(&path, error))?;
                Ok(entry.insert(writer))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct FinishedSpool {
    root: PathBuf,
    repaired: HashMap<(u32, SpoolKind), PathBuf>,
    pub(crate) repairs: Vec<SequenceRepair>,
    pub(crate) regressions: Vec<SequenceRegression>,
}

impl FinishedSpool {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn channels(&self) -> Result<Vec<u32>, ProductionError> {
        let mut channels = Vec::new();
        let entries =
            fs::read_dir(&self.root).map_err(|error| ProductionError::io(&self.root, error))?;
        for entry in entries {
            let entry = entry.map_err(|error| ProductionError::io(&self.root, error))?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            let Some(channel) = name
                .strip_prefix("channel-")
                .and_then(|rest| rest.split('.').next())
                .and_then(|value| value.parse::<u32>().ok())
            else {
                continue;
            };
            if !channels.contains(&channel) {
                channels.push(channel);
            }
        }
        channels.sort_unstable();
        Ok(channels)
    }

    pub(crate) fn sse_reader(&self, channel: u32) -> Result<SseReader, ProductionError> {
        SseReader::open(spool_path(&self.root, channel, SpoolKind::Sse))
    }

    pub(crate) fn sz_order_reader(
        &self,
        channel: u32,
    ) -> Result<Option<SzOrderReader>, ProductionError> {
        let path = self
            .repaired
            .get(&(channel, SpoolKind::SzOrder))
            .cloned()
            .unwrap_or_else(|| spool_path(&self.root, channel, SpoolKind::SzOrder));
        path.exists().then(|| SzOrderReader::open(path)).transpose()
    }

    pub(crate) fn sz_execution_reader(
        &self,
        channel: u32,
    ) -> Result<Option<SzExecutionReader>, ProductionError> {
        let path = self
            .repaired
            .get(&(channel, SpoolKind::SzExecution))
            .cloned()
            .unwrap_or_else(|| spool_path(&self.root, channel, SpoolKind::SzExecution));
        path.exists()
            .then(|| SzExecutionReader::open(path))
            .transpose()
    }

    pub(crate) fn cleanup(self) -> Result<(), ProductionError> {
        fs::remove_dir_all(&self.root).map_err(|error| ProductionError::io(&self.root, error))
    }
}

fn spool_path(root: &Path, channel: u32, kind: SpoolKind) -> PathBuf {
    root.join(format!("channel-{channel}.{}.bin", kind.suffix()))
}

macro_rules! reader {
    ($name:ident, $row:ty, $kind:expr, $read:ident) => {
        pub(crate) struct $name {
            path: PathBuf,
            reader: BufReader<File>,
        }

        impl $name {
            fn open(path: PathBuf) -> Result<Self, ProductionError> {
                let file = File::open(&path).map_err(|error| ProductionError::io(&path, error))?;
                let mut reader = BufReader::new(file);
                read_header(&mut reader, $kind)
                    .map_err(|error| ProductionError::io(&path, error))?;
                Ok(Self { path, reader })
            }

            pub(crate) fn next_row(&mut self) -> Result<Option<$row>, ProductionError> {
                $read(&mut self.reader).map_err(|error| ProductionError::io(&self.path, error))
            }
        }
    };
}

reader!(SseReader, SseRow, SpoolKind::Sse, read_sse);
reader!(SzOrderReader, SzOrderRow, SpoolKind::SzOrder, read_sz_order);
reader!(
    SzExecutionReader,
    SzExecutionRow,
    SpoolKind::SzExecution,
    read_sz_execution
);

fn read_header(reader: &mut impl Read, kind: SpoolKind) -> std::io::Result<()> {
    let mut header = [0_u8; 5];
    reader.read_exact(&mut header)?;
    if &header[..4] != MAGIC || header[4] != kind.byte() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid QTP spool header",
        ));
    }
    Ok(())
}

fn write_sse(writer: &mut impl Write, row: &SseRow) -> std::io::Result<()> {
    write_common(
        writer,
        row.source_row,
        row.sequence,
        row.channel,
        &row.symbol,
        row.quote_time_ns,
        row.local_time_ns,
    )?;
    write_u8(writer, sse_kind_byte(row.kind))?;
    write_u64(writer, row.buy_order_no)?;
    write_u64(writer, row.sell_order_no)?;
    write_i64(writer, row.price_units)?;
    write_u64(writer, row.quantity)?;
    write_u8(writer, aggressor_byte(row.flag))?;
    write_u8(writer, row.status)
}

fn read_sse(reader: &mut impl Read) -> std::io::Result<Option<SseRow>> {
    let Some((source_row, sequence, channel, symbol, quote_time_ns, local_time_ns)) =
        read_common(reader)?
    else {
        return Ok(None);
    };
    Ok(Some(SseRow {
        source_row,
        sequence,
        channel,
        symbol,
        quote_time_ns,
        local_time_ns,
        kind: read_sse_kind(read_u8(reader)?)?,
        buy_order_no: read_u64(reader)?,
        sell_order_no: read_u64(reader)?,
        price_units: read_i64(reader)?,
        quantity: read_u64(reader)?,
        flag: read_aggressor(read_u8(reader)?)?,
        status: read_u8(reader)?,
    }))
}

fn write_sz_order(writer: &mut impl Write, row: &SzOrderRow) -> std::io::Result<()> {
    write_common(
        writer,
        row.source_row,
        row.sequence,
        row.channel,
        &row.symbol,
        row.quote_time_ns,
        row.local_time_ns,
    )?;
    write_i64(writer, row.price_units)?;
    write_u64(writer, row.quantity)?;
    write_u8(
        writer,
        match row.side {
            SzSide::Buy => 1,
            SzSide::Sell => 2,
        },
    )?;
    write_u8(
        writer,
        match row.kind {
            SzOrderKind::Market => 1,
            SzOrderKind::Limit => 2,
            SzOrderKind::SameSideBest => 3,
        },
    )
}

fn read_sz_order(reader: &mut impl Read) -> std::io::Result<Option<SzOrderRow>> {
    let Some((source_row, sequence, channel, symbol, quote_time_ns, local_time_ns)) =
        read_common(reader)?
    else {
        return Ok(None);
    };
    let price_units = read_i64(reader)?;
    let quantity = read_u64(reader)?;
    let side = match read_u8(reader)? {
        1 => SzSide::Buy,
        2 => SzSide::Sell,
        _ => return invalid_data("invalid Shenzhen side"),
    };
    let kind = match read_u8(reader)? {
        1 => SzOrderKind::Market,
        2 => SzOrderKind::Limit,
        3 => SzOrderKind::SameSideBest,
        _ => return invalid_data("invalid Shenzhen order kind"),
    };
    Ok(Some(SzOrderRow {
        source_row,
        sequence,
        channel,
        symbol,
        quote_time_ns,
        local_time_ns,
        price_units,
        quantity,
        side,
        kind,
    }))
}

fn write_sz_execution(writer: &mut impl Write, row: &SzExecutionRow) -> std::io::Result<()> {
    write_common(
        writer,
        row.source_row,
        row.sequence,
        row.channel,
        &row.symbol,
        row.quote_time_ns,
        row.local_time_ns,
    )?;
    write_u64(writer, row.bid_order_no)?;
    write_u64(writer, row.ask_order_no)?;
    write_i64(writer, row.price_units)?;
    write_u64(writer, row.quantity)?;
    write_u8(
        writer,
        match row.kind {
            SzExecutionKind::Trade => 1,
            SzExecutionKind::Cancel => 2,
        },
    )
}

fn read_sz_execution(reader: &mut impl Read) -> std::io::Result<Option<SzExecutionRow>> {
    let Some((source_row, sequence, channel, symbol, quote_time_ns, local_time_ns)) =
        read_common(reader)?
    else {
        return Ok(None);
    };
    let bid_order_no = read_u64(reader)?;
    let ask_order_no = read_u64(reader)?;
    let price_units = read_i64(reader)?;
    let quantity = read_u64(reader)?;
    let kind = match read_u8(reader)? {
        1 => SzExecutionKind::Trade,
        2 => SzExecutionKind::Cancel,
        _ => return invalid_data("invalid Shenzhen execution kind"),
    };
    Ok(Some(SzExecutionRow {
        source_row,
        sequence,
        channel,
        symbol,
        quote_time_ns,
        local_time_ns,
        bid_order_no,
        ask_order_no,
        price_units,
        quantity,
        kind,
    }))
}

fn write_common(
    writer: &mut impl Write,
    source_row: u64,
    sequence: u64,
    channel: u32,
    symbol: &str,
    quote_time_ns: i64,
    local_time_ns: i64,
) -> std::io::Result<()> {
    write_u64(writer, source_row)?;
    write_u64(writer, sequence)?;
    write_u32(writer, channel)?;
    write_symbol(writer, symbol)?;
    write_i64(writer, quote_time_ns)?;
    write_i64(writer, local_time_ns)
}

type CommonRow = (u64, u64, u32, String, i64, i64);

fn read_common(reader: &mut impl Read) -> std::io::Result<Option<CommonRow>> {
    let Some(source_row) = read_first_u64(reader)? else {
        return Ok(None);
    };
    Ok(Some((
        source_row,
        read_u64(reader)?,
        read_u32(reader)?,
        read_symbol(reader)?,
        read_i64(reader)?,
        read_i64(reader)?,
    )))
}

fn write_symbol(writer: &mut impl Write, symbol: &str) -> std::io::Result<()> {
    if symbol.len() != 6 || !symbol.is_ascii() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "spool symbol must contain six ASCII bytes",
        ));
    }
    writer.write_all(symbol.as_bytes())
}

fn read_symbol(reader: &mut impl Read) -> std::io::Result<String> {
    let mut bytes = [0_u8; 6];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes.to_vec())
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid symbol"))
}

fn sse_kind_byte(value: SseKind) -> u8 {
    match value {
        SseKind::Add => 1,
        SseKind::Delete => 2,
        SseKind::Trade => 3,
        SseKind::Status => 4,
    }
}

fn read_sse_kind(value: u8) -> std::io::Result<SseKind> {
    match value {
        1 => Ok(SseKind::Add),
        2 => Ok(SseKind::Delete),
        3 => Ok(SseKind::Trade),
        4 => Ok(SseKind::Status),
        _ => invalid_data("invalid Shanghai event kind"),
    }
}

fn aggressor_byte(value: Aggressor) -> u8 {
    match value {
        Aggressor::Buy => 1,
        Aggressor::Sell => 2,
        Aggressor::Neutral => 3,
        Aggressor::Other => 4,
    }
}

fn read_aggressor(value: u8) -> std::io::Result<Aggressor> {
    match value {
        1 => Ok(Aggressor::Buy),
        2 => Ok(Aggressor::Sell),
        3 => Ok(Aggressor::Neutral),
        4 => Ok(Aggressor::Other),
        _ => invalid_data("invalid Shanghai flag"),
    }
}

fn write_u8(writer: &mut impl Write, value: u8) -> std::io::Result<()> {
    writer.write_all(&[value])
}

fn read_u8(reader: &mut impl Read) -> std::io::Result<u8> {
    let mut bytes = [0_u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn write_u32(writer: &mut impl Write, value: u32) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn read_u32(reader: &mut impl Read) -> std::io::Result<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn write_u64(writer: &mut impl Write, value: u64) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn read_first_u64(reader: &mut impl Read) -> std::io::Result<Option<u64>> {
    let mut bytes = [0_u8; 8];
    match reader.read(&mut bytes[..1])? {
        0 => Ok(None),
        1 => {
            reader.read_exact(&mut bytes[1..])?;
            Ok(Some(u64::from_le_bytes(bytes)))
        }
        _ => unreachable!(),
    }
}

fn read_u64(reader: &mut impl Read) -> std::io::Result<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn write_i64(writer: &mut impl Write, value: i64) -> std::io::Result<()> {
    writer.write_all(&value.to_le_bytes())
}

fn read_i64(reader: &mut impl Read) -> std::io::Result<i64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(i64::from_le_bytes(bytes))
}

fn invalid_data<T>(message: &'static str) -> std::io::Result<T> {
    Err(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message,
    ))
}

#[cfg(test)]
mod tests {
    use super::{Aggressor, SpoolSet, SseKind, SseReader, SseRow};

    #[test]
    fn round_trips_spool_row() {
        let directory = match tempfile::tempdir() {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let mut spool = match SpoolSet::create(directory.path(), "test") {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let row = SseRow {
            source_row: 1,
            sequence: 2,
            channel: 3,
            symbol: "600000".to_owned(),
            quote_time_ns: 4,
            local_time_ns: 5,
            kind: SseKind::Add,
            buy_order_no: 6,
            sell_order_no: 0,
            price_units: 70_000,
            quantity: 8,
            flag: Aggressor::Buy,
            status: 0,
        };
        assert!(spool.write_sse(&row).is_ok());
        let finished = match spool.finish() {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        let mut reader = match SseReader::open(finished.root().join("channel-3.sse.bin")) {
            Ok(value) => value,
            Err(_) => std::process::abort(),
        };
        assert_eq!(reader.next_row().ok().flatten(), Some(row));
        assert_eq!(reader.next_row().ok().flatten(), None);
    }
}
