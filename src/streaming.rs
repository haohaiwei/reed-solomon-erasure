//! Streaming encoder/decoder for Reed-Solomon erasure coding.
//!
//! This module provides [`StreamEncoder`] which can encode, verify, and
//! reconstruct data that is streamed through I/O readers and writers,
//! processing one block at a time rather than requiring the entire dataset
//! to be in memory.

use crate::errors::Error;
use crate::Field;
use crate::ReedSolomon;
use thiserror::Error;

/// Errors that can occur during streaming operations.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum StreamError {
    /// An I/O error occurred while reading.
    #[error("I/O read error")]
    Read(#[source] std::io::Error),
    /// An I/O error occurred while writing.
    #[error("I/O write error")]
    Write(#[source] std::io::Error),
    /// A Reed-Solomon error occurred.
    #[error("{0}")]
    RSError(#[from] Error),
}

/// A streaming encoder/decoder for Reed-Solomon erasure coding.
///
/// Unlike the base [`ReedSolomon`] which operates on in-memory slices,
/// `StreamEncoder` reads data from readers and writes parity/reconstructed
/// data to writers, processing `block_size` bytes at a time. This allows
/// encoding and decoding of data that is too large to fit in memory, or
/// that arrives incrementally.
#[derive(Debug)]
pub struct StreamEncoder<F: Field> {
    inner: ReedSolomon<F>,
    block_size: usize,
}

impl<F: Field> StreamEncoder<F> {
    /// Creates a new `StreamEncoder` with the given number of data and parity
    /// shards and a default block size of 4096 bytes.
    ///
    /// # Panics
    ///
    /// Panics if `F::Elem` is not `u8`.
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self, Error>
    where
        F::Elem: 'static,
    {
        assert!(
            std::any::TypeId::of::<F::Elem>() == std::any::TypeId::of::<u8>(),
            "StreamEncoder only supports u8 field elements"
        );
        Ok(Self {
            inner: ReedSolomon::new(data_shards, parity_shards)?,
            block_size: 4096,
        })
    }

    /// Sets the block size for streaming operations.
    ///
    /// The block size determines how many bytes are read from each shard
    /// per iteration. Larger block sizes may improve throughput at the cost
    /// of higher memory usage.
    ///
    /// # Panics
    ///
    /// Panics if `block_size` is 0.
    #[must_use]
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        assert!(block_size > 0, "block_size must be greater than 0");
        self.block_size = block_size;
        self
    }

    /// Returns the number of data shards.
    #[must_use]
    pub fn data_shard_count(&self) -> usize {
        self.inner.data_shard_count()
    }

    /// Returns the number of parity shards.
    #[must_use]
    pub fn parity_shard_count(&self) -> usize {
        self.inner.parity_shard_count()
    }

    /// Returns the total number of shards (data + parity).
    #[must_use]
    pub fn total_shard_count(&self) -> usize {
        self.inner.total_shard_count()
    }

    /// Returns the block size used for streaming operations.
    #[must_use]
    pub fn block_size(&self) -> usize {
        self.block_size
    }

    /// Encodes data by reading blocks from the data readers and writing
    /// computed parity blocks to the parity writers.
    ///
    /// The `data` slice must contain exactly `data_shard_count()` readers,
    /// and the `parity` slice must contain exactly `parity_shard_count()`
    /// writers.
    ///
    /// Each reader is read until it returns zero bytes (EOF). If any reader
    /// ends before the others, its remaining bytes are filled with zeroes.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Read`] if an I/O error occurs while reading
    /// from a data shard reader.
    ///
    /// Returns [`StreamError::Write`] if an I/O error occurs while writing
    /// to a parity shard writer.
    ///
    /// Returns [`StreamError::RSError`] if the number of readers or writers
    /// does not match the encoder configuration.
    ///
    /// # Panics
    ///
    /// Panics if `block_size` is 0 (should not happen as it is checked
    /// during construction).
    #[cfg(feature = "std")]
    pub fn encode<R, W>(&self, data: &mut [R], parity: &mut [W]) -> Result<(), StreamError>
    where
        R: std::io::Read,
        W: std::io::Write,
    {
        if data.len() != self.data_shard_count() {
            return Err(StreamError::RSError(Error::TooFewDataShards));
        }
        if parity.len() != self.parity_shard_count() {
            return Err(StreamError::RSError(Error::TooFewParityShards));
        }

        let block_size = self.block_size;
        let data_shard_count = self.data_shard_count();

        // Allocate buffers: one per data shard + one per parity shard.
        let mut buf: Vec<Vec<F::Elem>> = (0..self.total_shard_count())
            .map(|_| vec![F::zero(); block_size])
            .collect();

        loop {
            // Restore buffer capacity for all shards (shrink from previous
            // short reads) by resizing back to block_size.
            for b in &mut buf {
                b.resize(self.block_size, F::zero());
            }

            // Read one block from each data shard reader and zero-fill
            // the unread portion.
            let mut read_counts = vec![0usize; data_shard_count];
            for i in 0..data_shard_count {
                read_counts[i] = read_into::<R, F>(&mut data[i], &mut buf[i])?;
                for elem in buf[i][read_counts[i]..].iter_mut() {
                    *elem = F::zero();
                }
            }

            if read_counts.iter().all(|&c| c == 0) {
                break;
            }

            // Find the maximum bytes read this round and truncate all
            // buffers to that length.
            let max_read = *read_counts.iter().max().unwrap_or(&0);
            for b in &mut buf {
                b.truncate(max_read);
            }

            // Encode this block.
            self.inner.encode(&mut buf)?;

            // Write parity blocks.
            for i in 0..self.parity_shard_count() {
                write_all_from::<W, F>(&mut parity[i], &buf[data_shard_count + i])?;
            }
        }

        Ok(())
    }

    /// Verifies that all shards are consistent by reading blocks from all
    /// shard readers and checking the parity.
    ///
    /// Returns `Ok(true)` if all blocks verify successfully, or
    /// `Ok(false)` if any block fails verification.
    ///
    /// The `shards` slice must contain exactly `total_shard_count()` readers.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Read`] if an I/O error occurs while reading
    /// from a shard reader.
    ///
    /// Returns [`StreamError::RSError`] if the number of readers does not
    /// match the total shard count.
    ///
    /// # Panics
    ///
    /// Panics if `block_size` is 0 (should not happen as it is checked
    /// during construction).
    #[cfg(feature = "std")]
    pub fn verify<R>(&self, shards: &mut [R]) -> Result<bool, StreamError>
    where
        R: std::io::Read,
    {
        if shards.len() != self.total_shard_count() {
            return Err(StreamError::RSError(Error::TooFewShards));
        }

        let block_size = self.block_size;
        let total = self.total_shard_count();

        let mut buf: Vec<Vec<F::Elem>> =
            (0..total).map(|_| vec![F::zero(); block_size]).collect();

        loop {
            for b in &mut buf {
                b.resize(self.block_size, F::zero());
            }

            let mut read_counts = vec![0usize; total];
            for i in 0..total {
                read_counts[i] = read_into::<R, F>(&mut shards[i], &mut buf[i])?;
                for elem in buf[i][read_counts[i]..].iter_mut() {
                    *elem = F::zero();
                }
            }

            if read_counts.iter().all(|&c| c == 0) {
                break;
            }

            let max_read = *read_counts.iter().max().unwrap_or(&0);

            for b in &mut buf {
                b.truncate(max_read);
            }

            if max_read == 0 {
                continue;
            }

            if !self.inner.verify(&buf)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Reconstructs missing shards by reading available shards and writing
    /// reconstructed shards to the corresponding writers.
    ///
    /// The `valid` slice must contain exactly `total_shard_count()` entries,
    /// where `Some(reader)` indicates an available shard and `None` indicates
    /// a missing shard. The `fill` slice must contain exactly
    /// `total_shard_count()` entries, where `Some(writer)` corresponds to a
    /// missing shard that will receive reconstructed data, and `None` for
    /// shards that are already present.
    ///
    /// The same index must have `Some(reader)` in `valid` if and only if it
    /// has `None` in `fill`, and vice versa.
    ///
    /// # Errors
    ///
    /// Returns [`StreamError::Read`] if an I/O error occurs while reading
    /// from a valid shard reader.
    ///
    /// Returns [`StreamError::Write`] if an I/O error occurs while writing
    /// to a fill shard writer.
    ///
    /// Returns [`StreamError::RSError`] if there are too few valid shards
    /// to reconstruct, or if the shard counts are incorrect.
    ///
    /// # Panics
    ///
    /// Panics if `block_size` is 0 (should not happen as it is checked
    /// during construction).
    #[cfg(feature = "std")]
    pub fn reconstruct<R, W>(
        &self,
        valid: &mut [Option<R>],
        fill: &mut [Option<W>],
    ) -> Result<(), StreamError>
    where
        R: std::io::Read,
        W: std::io::Write,
    {
        if valid.len() != self.total_shard_count() {
            return Err(StreamError::RSError(Error::TooFewShards));
        }
        if fill.len() != self.total_shard_count() {
            return Err(StreamError::RSError(Error::TooFewShards));
        }

        let block_size = self.block_size;
        let total = self.total_shard_count();

        // Use (Vec<F::Elem>, bool) as ReconstructShard so the Vec allocation
        // is reused across iterations. The bool indicates whether the shard
        // is present (true) or missing (false).
        let mut shards: Vec<(Vec<F::Elem>, bool)> = (0..total)
            .map(|i| {
                if valid[i].is_some() {
                    (vec![F::zero(); block_size], true)
                } else {
                    (Vec::new(), false)
                }
            })
            .collect();

        loop {
            // Resize valid shard buffers to block_size for reading.
            for shard in &mut shards {
                if shard.1 {
                    shard.0.resize(self.block_size, F::zero());
                }
            }

            // Read one block from each valid shard, zero-fill unread portions.
            let mut read_counts = vec![0usize; total];
            for i in 0..total {
                if valid[i].is_some() && shards[i].1 {
                    read_counts[i] = read_into::<R, F>(
                        valid[i].as_mut().expect("just checked is_some"),
                        &mut shards[i].0,
                    )?;
                    for elem in shards[i].0[read_counts[i]..].iter_mut() {
                        *elem = F::zero();
                    }
                }
            }

            if read_counts.iter().all(|&c| c == 0) {
                break;
            }

            let max_read = *read_counts.iter().max().unwrap_or(&0);

            // Truncate ALL shard buffers to max_read. For missing shards,
            // also resize from empty to max_read (zero-fill) so that
            // ReconstructShard::get_or_initialize sees the correct size.
            for shard in &mut shards {
                shard.0.truncate(max_read);
                if !shard.1 && shard.0.len() < max_read {
                    shard.0.resize(max_read, F::zero());
                }
            }

            if max_read == 0 {
                continue;
            }

            // Reconstruct missing shards.
            self.inner.reconstruct(&mut shards)?;

            // Write reconstructed shards to fill writers.
            for i in 0..total {
                // valid[i].is_none() means this shard was originally missing
                // and has been reconstructed by reconstruct().
                if fill[i].is_some() && valid[i].is_none() {
                    write_all_from::<W, F>(fill[i].as_mut().expect("just checked is_some"), &shards[i].0)?;
                }
            }

            // Swap-based approach: reset reconstructed shards back to
            // "missing" state. The (vec, false) tuple means the Vec
            // allocation is retained and reused next iteration, but the
            // bool tells reconstruct this shard is missing again.
            for i in 0..total {
                if valid[i].is_none() && shards[i].1 {
                    shards[i].0.clear();
                    shards[i].1 = false;
                }
            }
        }

        Ok(())
    }
}

/// Reads up to `buf.len()` bytes from `reader` into `buf`, treating the
/// buffer as a slice of `F::Elem` (which must be `u8`).
///
/// After reading, the portion of the buffer beyond the read bytes is
/// **not** zeroed by this function; the caller must do so if needed.
///
/// Returns the number of bytes actually read.
///
/// # Errors
///
/// Returns [`StreamError::Read`] if the underlying read fails.
#[cfg(feature = "std")]
fn read_into<R: std::io::Read, F: Field>(
    reader: &mut R,
    buf: &mut Vec<F::Elem>,
) -> Result<usize, StreamError> {
    // Safety: F::Elem is asserted to be u8 in StreamEncoder::new, so
    // we can safely transmute the Vec content for I/O.
    let byte_slice = unsafe {
        std::slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, buf.len())
    };
    let mut total = 0;
    while total < byte_slice.len() {
        match reader.read(&mut byte_slice[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(StreamError::Read(e)),
        }
    }
    Ok(total)
}

/// Writes all bytes from `buf` (treated as `u8` slice) to `writer`.
///
/// # Errors
///
/// Returns [`StreamError::Write`] if the underlying write fails.
#[cfg(feature = "std")]
fn write_all_from<W: std::io::Write, F: Field>(
    writer: &mut W,
    buf: &[F::Elem],
) -> Result<(), StreamError> {
    let byte_slice =
        unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, buf.len()) };
    writer.write_all(byte_slice).map_err(StreamError::Write)
}

/// Shared implementation for reading one block from each present shard reader
/// into the corresponding element buffer.
///
/// Returns a vector of read counts, one per shard (0 for missing shards).
///
/// This is the deduplicated core used by both the non-optional and optional
/// wrappers.
///
/// # Errors
///
/// Returns [`StreamError::Read`] if any read fails.
#[cfg(feature = "std")]
fn read_elem_shards_impl<R: std::io::Read, F: Field>(
    readers: &mut [Option<R>],
    shards: &mut [Option<Vec<F::Elem>>],
) -> Result<Vec<usize>, StreamError> {
    let total = shards.len();
    let mut read_counts = vec![0usize; total];

    for i in 0..total {
        if let (Some(ref mut reader), Some(ref mut buf)) =
            (readers.get_mut(i).and_then(|opt| opt.as_mut()), &mut shards[i])
        {
            read_counts[i] = read_into::<R, F>(reader, buf)?;
        }
    }

    Ok(read_counts)
}

/// Reads one block from each present element shard reader (non-optional).
///
/// Thin wrapper around [`read_elem_shards_impl`].
///
/// # Errors
///
/// Returns [`StreamError::Read`] if any read fails.
#[cfg(feature = "std")]
#[allow(dead_code)]
fn read_elem_shards<R: std::io::Read, F: Field>(
    readers: &mut [R],
    shards: &mut [Option<Vec<F::Elem>>],
) -> Result<Vec<usize>, StreamError> {
    let mut opt_readers: Vec<Option<&mut R>> = readers.iter_mut().map(Some).collect();
    read_elem_shards_impl::<&mut R, F>(&mut opt_readers, shards)
}

/// Reads one block from each present optional element shard reader.
///
/// Thin wrapper around [`read_elem_shards_impl`].
///
/// # Errors
///
/// Returns [`StreamError::Read`] if any read fails.
#[cfg(feature = "std")]
#[allow(dead_code)]
fn read_optional_elem_shards<R: std::io::Read, F: Field>(
    readers: &mut [Option<R>],
    shards: &mut [Option<Vec<F::Elem>>],
) -> Result<Vec<usize>, StreamError> {
    read_elem_shards_impl::<R, F>(readers, shards)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::galois_8::Field as GF8;
    use std::io::Cursor;

    fn make_encoder(data: usize, parity: usize, block_size: usize) -> StreamEncoder<GF8> {
        StreamEncoder::new(data, parity).unwrap().with_block_size(block_size)
    }

    #[test]
    fn encode_basic_round_trip() {
        let encoder = make_encoder(3, 2, 4);

        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let data2: Vec<u8> = vec![100, 200, 150, 50, 25, 75, 125, 175];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut r2 = Cursor::new(data2.clone());

        let mut parity0 = Vec::new();
        let mut parity1 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1, &mut r2], &mut [&mut parity0, &mut parity1])
            .unwrap();

        // Verify using the base ReedSolomon
        let rs = ReedSolomon::<GF8>::new(3, 2).unwrap();
        let shards = vec![data0, data1, data2, parity0, parity1];
        assert!(rs.verify(&shards).unwrap());
    }

    #[test]
    fn reconstruct_basic() {
        let encoder = make_encoder(3, 2, 4);

        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let data2: Vec<u8> = vec![100, 200, 150, 50, 25, 75, 125, 175];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut r2 = Cursor::new(data2.clone());

        let mut parity0 = Vec::new();
        let mut parity1 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1, &mut r2], &mut [&mut parity0, &mut parity1])
            .unwrap();

        // Now reconstruct with shard 0 missing
        let mut valid: Vec<Option<Cursor<Vec<u8>>>> = vec![
            None,
            Some(Cursor::new(data1.clone())),
            Some(Cursor::new(data2.clone())),
            Some(Cursor::new(parity0.clone())),
            Some(Cursor::new(parity1.clone())),
        ];
        let mut fill: Vec<Option<Vec<u8>>> = vec![Some(Vec::new()), None, None, None, None];

        encoder.reconstruct(&mut valid, &mut fill).unwrap();

        assert_eq!(fill[0].take().unwrap(), data0);
    }

    #[test]
    fn verify_valid_data() {
        let encoder = make_encoder(3, 2, 4);

        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let data2: Vec<u8> = vec![100, 200, 150, 50, 25, 75, 125, 175];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut r2 = Cursor::new(data2.clone());

        let mut parity0 = Vec::new();
        let mut parity1 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1, &mut r2], &mut [&mut parity0, &mut parity1])
            .unwrap();

        let mut shards: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(data0),
            Cursor::new(data1),
            Cursor::new(data2),
            Cursor::new(parity0),
            Cursor::new(parity1),
        ];

        assert!(encoder.verify(&mut shards).unwrap());
    }

    #[test]
    fn verify_corrupted_data() {
        let encoder = make_encoder(3, 2, 4);

        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let data2: Vec<u8> = vec![100, 200, 150, 50, 25, 75, 125, 175];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut r2 = Cursor::new(data2.clone());

        let mut parity0 = Vec::new();
        let mut parity1 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1, &mut r2], &mut [&mut parity0, &mut parity1])
            .unwrap();

        // Corrupt parity0
        let mut corrupted_parity0 = parity0.clone();
        corrupted_parity0[0] ^= 0xFF;

        let mut shards: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(data0),
            Cursor::new(data1),
            Cursor::new(data2),
            Cursor::new(corrupted_parity0),
            Cursor::new(parity1),
        ];

        assert!(!encoder.verify(&mut shards).unwrap());
    }

    #[test]
    fn zero_length_streams() {
        let encoder = make_encoder(2, 1, 4);

        let mut r0 = Cursor::new(Vec::<u8>::new());
        let mut r1 = Cursor::new(Vec::<u8>::new());
        let mut parity0 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1], &mut [&mut parity0])
            .unwrap();

        // Parity should also be empty for zero-length input
        assert!(parity0.is_empty());

        // Verify should succeed on empty streams
        let mut shards: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        ];
        assert!(encoder.verify(&mut shards).unwrap());
    }

    #[test]
    fn block_size_one() {
        let encoder = make_encoder(2, 1, 1);

        let data0: Vec<u8> = vec![42, 99, 7];
        let data1: Vec<u8> = vec![13, 88, 200];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut parity0 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1], &mut [&mut parity0])
            .unwrap();

        let rs = ReedSolomon::<GF8>::new(2, 1).unwrap();
        let shards = vec![data0, data1, parity0];
        assert!(rs.verify(&shards).unwrap());
    }

    #[test]
    #[should_panic(expected = "block_size must be greater than 0")]
    fn block_size_zero_panics() {
        let _ = StreamEncoder::<GF8>::new(2, 1).unwrap().with_block_size(0);
    }

    #[test]
    fn overlapping_valid_and_fill() {
        let encoder = make_encoder(3, 2, 4);

        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30, 40, 50, 60, 70, 80];
        let data2: Vec<u8> = vec![100, 200, 150, 50, 25, 75, 125, 175];

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut r2 = Cursor::new(data2.clone());

        let mut parity0 = Vec::new();
        let mut parity1 = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1, &mut r2], &mut [&mut parity0, &mut parity1])
            .unwrap();

        // Reconstruct with shards 0 and 4 missing
        let mut valid: Vec<Option<Cursor<Vec<u8>>>> = vec![
            None,
            Some(Cursor::new(data1.clone())),
            Some(Cursor::new(data2.clone())),
            Some(Cursor::new(parity0.clone())),
            None,
        ];
        let mut fill: Vec<Option<Vec<u8>>> =
            vec![Some(Vec::new()), None, None, None, Some(Vec::new())];

        encoder.reconstruct(&mut valid, &mut fill).unwrap();

        assert_eq!(fill[0].take().unwrap(), data0);
        // Also verify that parity1 was reconstructed
        let rs = ReedSolomon::<GF8>::new(3, 2).unwrap();
        let reconstructed_parity1 = fill[4].take().unwrap();
        let shards = vec![data0, data1, data2, parity0, reconstructed_parity1];
        assert!(rs.verify(&shards).unwrap());
    }

    #[test]
    fn zero_length_verify() {
        let encoder = make_encoder(2, 1, 4);

        // All empty shards should verify as true
        let mut shards: Vec<Cursor<Vec<u8>>> = vec![
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
            Cursor::new(Vec::new()),
        ];
        assert!(encoder.verify(&mut shards).unwrap());
    }

    #[test]
    fn unequal_data_lengths_zero_fill() {
        let encoder = make_encoder(2, 1, 4);

        // One shard shorter than the other
        let data0: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data1: Vec<u8> = vec![10, 20, 30]; // shorter

        let mut r0 = Cursor::new(data0.clone());
        let mut r1 = Cursor::new(data1.clone());
        let mut parity = Vec::new();

        encoder
            .encode(&mut [&mut r0, &mut r1], &mut [&mut parity])
            .unwrap();

        // The short shard should have been zero-filled, so parity should
        // be computed as if data1 = [10, 20, 30, 0, 0, 0, 0, 0]
        let rs = ReedSolomon::<GF8>::new(2, 1).unwrap();
        let padded_data1: Vec<u8> = vec![10, 20, 30, 0, 0, 0, 0, 0];
        let shards = vec![data0, padded_data1, parity];
        assert!(rs.verify(&shards).unwrap());
    }
}