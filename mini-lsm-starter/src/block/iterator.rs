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

use crate::key::{KeySlice, KeyVec};
use bytes::Buf;
use std::sync::Arc;

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut block_iterator = Self::new(block);
        block_iterator.seek_to_first();
        block_iterator
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut block_iterator = Self::new(block);
        block_iterator.seek_to_key(key);
        block_iterator
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
        self.seek_to_idx(0);
        if self.first_key.is_empty() {
            self.first_key = self.key.clone();
        }
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx = self.idx + 1;
        self.seek_to_idx(self.idx);
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let data_begin = self.block.offsets[idx] as usize;
        // data_begin = data[key_len,key,value_len,value]
        let key_len = (&self.block.data[data_begin..]).get_u16() as usize;
        self.key.clear();
        self.key
            .append(&self.block.data[data_begin + SIZEOF_U16..data_begin + SIZEOF_U16 + key_len]);
        let value_len = (&self.block.data[data_begin + SIZEOF_U16 + key_len..]).get_u16() as usize;
        self.value_range = (
            data_begin + SIZEOF_U16 + key_len + SIZEOF_U16,
            data_begin + SIZEOF_U16 + key_len + SIZEOF_U16 + value_len,
        );
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        self.idx = 0;
        self.seek_to_idx(0);
        while self.is_valid() {
            let tmp = String::from_utf8(self.key.raw_ref().to_vec());
            if self.key >= key.to_key_vec() {
                break;
            }
            self.next();
        }
    }
}
