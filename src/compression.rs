use clap::ValueEnum;
use std::io::Write;

#[derive(Clone, Copy, ValueEnum, Debug)]
pub enum Compression {
    Gzip,
    Zstd,
}

impl Compression {
    pub fn extension(&self) -> &'static str {
        match self {
            Compression::Gzip => "gz",
            Compression::Zstd => "zstd",
        }
    }

    pub fn get_encoder(&self, output_buffer: Vec<u8>) -> Encoder {
        match self {
            Compression::Gzip => {
                let encoder =
                    flate2::write::GzEncoder::new(output_buffer, flate2::Compression::default());
                Encoder::Gzip(encoder)
            }
            Compression::Zstd => {
                let encoder =
                    zstd::Encoder::new(output_buffer, zstd::DEFAULT_COMPRESSION_LEVEL).unwrap();
                Encoder::Zstd(encoder)
            }
        }
    }
}

pub enum Encoder {
    Gzip(flate2::write::GzEncoder<Vec<u8>>),
    Zstd(zstd::Encoder<'static, Vec<u8>>),
}

impl Encoder {
    pub fn finish(self) -> std::io::Result<Vec<u8>> {
        match self {
            Encoder::Gzip(e) => e.finish(),
            Encoder::Zstd(e) => e.finish(),
        }
    }
}

impl Write for Encoder {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Encoder::Gzip(e) => e.write(buf),
            Encoder::Zstd(e) => e.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Encoder::Gzip(e) => e.flush(),
            Encoder::Zstd(e) => e.flush(),
        }
    }
}
