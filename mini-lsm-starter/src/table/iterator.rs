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

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let blk_idx = 0;
        let block = table.read_block(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.blk_idx = 0;
        let block = self.table.read_block(self.blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        self.blk_iter = blk_iter;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let blk_idx = 0;
        let block = table.read_block(blk_idx)?;
        let blk_iter = BlockIterator::create_and_seek_to_first(block);
        let mut ssTable_iterator = SsTableIterator {
            table,
            blk_iter,
            blk_idx,
        };
        ssTable_iterator.seek_to_key(key)?;
        Ok(ssTable_iterator)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        pub struct TSsTableIterator {
            table: Arc<SsTable>,
            blk_iter: BlockIterator,
            blk_idx: usize,
        }
        if key.raw_ref().lt(self.table.first_key().raw_ref())
            || key.raw_ref().gt(self.table.last_key().raw_ref())
        {
            if key.raw_ref().lt(self.table.first_key().raw_ref()) {
                self.blk_idx = 0;
            }
            let block = self.table.read_block(self.blk_idx)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        } else {
            let mut left_index = 0;
            let mut right_index = self.table.block_meta.len() - 1;
            while left_index < right_index {
                let mid_index = left_index + (right_index - left_index) / 2;
                let tmp = String::from_utf8(
                    self.table.block_meta[mid_index]
                        .first_key
                        .raw_ref()
                        .to_vec(),
                );
                if key
                    .raw_ref()
                    .ge(self.table.block_meta[mid_index].first_key.raw_ref())
                    && key
                        .raw_ref()
                        .le(self.table.block_meta[mid_index].last_key.raw_ref())
                {
                    left_index = mid_index;
                    break;
                }
                if self.table.block_meta[mid_index]
                    .first_key
                    .raw_ref()
                    .lt(key.raw_ref())
                {
                    left_index = mid_index + 1;
                } else {
                    right_index = mid_index - 1;
                }
            }
            let block = self.table.read_block(left_index)?;
            self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
            // 范围内可能没有，需要 seek 到下一个 range
            if !self.blk_iter.is_valid() {
                let block = self.table.read_block(left_index + 1)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid() || self.blk_idx < self.table.block_meta.len()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if self.blk_iter.is_valid() {
            Ok(())
        } else {
            self.blk_idx += 1;
            if self.is_valid() {
                let block = self.table.read_block(self.blk_idx)?;
                self.blk_iter = BlockIterator::create_and_seek_to_first(block);
            }
            Ok(())
        }
    }
}
