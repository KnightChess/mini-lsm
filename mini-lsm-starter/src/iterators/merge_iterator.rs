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

use super::StorageIterator;
use crate::key::KeySlice;
use anyhow::Result;
use std::cmp::{self};
use std::collections::BinaryHeap;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            MergeIterator {
                iters: BinaryHeap::new(),
                current: None,
            }
        } else {
            let filter_vec: Vec<_> = iters.into_iter().filter(|i| i.is_valid()).collect();

            let mut iter_heap = vec![];
            for iterator in filter_vec.into_iter().enumerate() {
                iter_heap.push(HeapWrapper(iterator.0, iterator.1))
            }
            let mut heap = BinaryHeap::from(iter_heap);
            let current = heap.pop().take();
            MergeIterator {
                iters: heap,
                current,
            }
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() && self.current.as_ref().unwrap().1.as_ref().is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // 需要对 key 进行 clone，不产生不可变借用
        let mut cur_key = self.current.as_ref().unwrap().1.key().to_key_vec();
        // 需要进行 take 或者 mem::replace，转移所有权，不然 self.current.unwrap 会把结构体的 current 所有权移动
        // 应该是 self 是可变借用了，表示有别的所有者了，然后结构体是单一所有者，所以不能转移所有权
        let str = String::from_utf8(cur_key.raw_ref().to_vec())?;
        self.iters.push(self.current.take().unwrap());
        loop {
            if let Some(mut item) = self.iters.pop() {
                if !item.1.is_valid() {
                    continue;
                }
                let key = String::from_utf8(item.1.key().raw_ref().to_vec())?;
                let value = String::from_utf8(item.1.value().to_vec())?;
                if cur_key == item.1.key().to_key_vec() {
                    // 不用 ？需要手动处理是因为，Err 了也得对 heapwrapper 进行 pop，否则离开作用域会自动构建 heap，调用 key，会命中 error_when 的逻辑
                    if let e @ Err(_) = item.1.next() {
                        return e;
                    }
                    if item.1.is_valid() {
                        self.iters.push(item);
                    }
                } else {
                    if item.1.value().is_empty() {
                        cur_key = item.1.key().to_key_vec();
                        self.iters.push(item);
                    } else {
                        self.iters.push(item);
                        break;
                    }
                }
            } else {
                break;
            }
        }
        if self.iters.peek().is_some() && self.iters.peek().unwrap().1.is_valid() {
            self.current = self.iters.pop();
        } else {
            self.current = None
        }

        Ok(())
    }
}
