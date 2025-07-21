// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        buf.put_u32(block_meta.len() as u32);
        block_meta.iter().for_each(|m| {
            buf.put_u32(m.offset as u32);
            buf.put_u16(m.first_key.len() as u16);
            buf.put_slice(m.first_key.raw_ref());
            buf.put_u16(m.last_key.len() as u16);
            buf.put_slice(m.last_key.raw_ref());
        })
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut buffer = buf.chunk();
        let mut block_meta = Vec::new();
        let num = buffer.get_u32() as usize;
          while buffer.has_remaining() {
            let offset = buffer.get_u32() as usize;
            let first_key_len = buffer.get_u16() as usize;
            let first_key = KeyBytes::from_bytes(buffer.copy_to_bytes(first_key_len));
            let last_key_len = buffer.get_u16() as usize;
            let last_key = KeyBytes::from_bytes(buffer.copy_to_bytes(last_key_len));
            let b_m = BlockMeta {
                offset,
                first_key,
                last_key,
            };
            block_meta.push(b_m);
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let file_len = file.size();
        let raw_meta_offset = file.read(file_len -  SIZEOF_U32 as u64, SIZEOF_U32 as u64)?;
        let block_meta_offset = (&raw_meta_offset[..]).get_u32() as usize;
        let raw_data = file.read(block_meta_offset as u64, file_len - block_meta_offset as u64 - SIZEOF_U32 as u64)?;
        let block_meta = BlockMeta::decode_block_meta(&raw_data[..]);
        let mut first_key = KeyBytes::from_bytes(Bytes::new());
        let mut last_key = KeyBytes::from_bytes(Bytes::new());
        if !block_meta.is_empty() {
            first_key = (&block_meta[0].first_key).clone();
            last_key = (&block_meta[block_meta.len() - 1].last_key).clone();
        }
        let sst = SsTable {
            file,
            block_meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        };
        Ok(sst)
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let mut block_end = self.block_meta_offset;
        //todo block idx >= meta len
        if block_idx < (self.block_meta.len() - 1) {
            block_end = self.block_meta[block_idx].offset
        }
        let target_block_meta = &self.block_meta[block_idx];
        let data_len = block_end - target_block_meta.offset;
        let raw_data = self.file.read(target_block_meta.offset as u64, data_len as u64)?;
        Ok(Arc::new(Block::decode(&raw_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut idx = 0;
        if self.first_key.raw_ref() > key.raw_ref() || self.last_key.raw_ref() < key.raw_ref() {
            return idx;
        }
        for meta in self.block_meta.iter() {
            if meta.last_key.raw_ref() < key.raw_ref() {
                idx = idx + 1;
                continue;
            }
            break;
        }
        idx
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
