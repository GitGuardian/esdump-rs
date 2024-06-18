use human_duration::human_duration;
use std::fmt::{Display, Formatter};
use std::ops::AddAssign;
use std::time::Duration;

#[derive(Default)]
pub struct BatchStats {
    pub elasticsearch: Duration,
    pub compression: Duration,
    pub storage: Duration,
    pub buffers: Duration,
    pub json: Duration,
    pub elasticsearch_bytes: u64,
    pub compressed_bytes: u64,
}

impl AddAssign for BatchStats {
    fn add_assign(&mut self, rhs: Self) {
        self.elasticsearch += rhs.elasticsearch;
        self.compression += rhs.compression;
        self.storage += rhs.storage;
        self.buffers += rhs.buffers;
        self.json += rhs.json;
        self.elasticsearch_bytes += rhs.elasticsearch_bytes;
        self.compressed_bytes += rhs.compressed_bytes;
    }
}

impl Display for BatchStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "elasticsearch: {}", human_duration(&self.elasticsearch))?;
        write!(f, " / compression: {}", human_duration(&self.compression))?;
        write!(f, " / json: {}", human_duration(&self.json))?;
        write!(f, " / storage: {}", human_duration(&self.storage))?;
        write!(f, " / buffers: {}", human_duration(&self.buffers))?;
        write!(
            f,
            " / elasticsearch bytes: {}",
            bytesize::ByteSize(self.elasticsearch_bytes)
        )?;
        write!(
            f,
            " / compressed bytes: {}",
            bytesize::ByteSize(self.compressed_bytes)
        )?;
        Ok(())
    }
}
