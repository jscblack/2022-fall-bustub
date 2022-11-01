//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.resize((1 << global_depth_));
  dir_.at(0) = std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_, global_depth_);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}
template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key, int depth) -> size_t {
  int mask = (1 << depth) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_.at(dir_index)->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  if (idx >= dir_.size()) {
    value = {};
    return false;
  }
  return dir_.at(idx)->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  if (idx >= dir_.size()) {
    return false;
  }
  return dir_.at(idx)->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t idx = IndexOf(key);
  if (idx >= dir_.size()) {
    return;
  }
  while (!dir_.at(idx)->Insert(key, value)) {
    if (dir_.at(idx)->GetDepth() == global_depth_) {
      // need more slots
      std::vector<std::shared_ptr<Bucket>> dir_tmp = dir_;
      dir_.resize(1 << (global_depth_ + 1));
      for (size_t i = 0; i < dir_.size(); i++) {
        dir_.at(i) = dir_tmp.at(i & ((1 << global_depth_) - 1));  // relocate
      }
      global_depth_++;
    }
    idx = IndexOf(key);  // go to idx
    // split this list
    dir_.at(idx)->IncrementDepth();
    std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> list_1 = dir_.at(idx);
    std::shared_ptr<ExtendibleHashTable<K, V>::Bucket> list_2 =
        std::make_shared<ExtendibleHashTable<K, V>::Bucket>(bucket_size_, dir_.at(idx)->GetDepth());
    num_buckets_++;
    int bucket_depth = dir_.at(idx)->GetDepth();
    for (auto it = list_1->GetItems().begin(); it != list_1->GetItems().end();) {
      if (IndexOf(it->first, bucket_depth) != IndexOf(it->first, bucket_depth - 1)) {
        // need to reshape this
        list_2->Insert(it->first, it->second);
        it = list_1->GetItems().erase(it);
      } else {
        it++;
      }
    }
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_.at(i).get() == list_1.get()) {
        // this slot was connected to list_1
        // check if it needs to connect to list_2
        if (((i) & ((1 << bucket_depth) - 1)) != ((i) & ((1 << (bucket_depth - 1)) - 1))) {
          dir_.at(i) = list_2;
        }
      }
    }
  }
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//

template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &elem : list_) {
    if (elem.first == key) {
      value = elem.second;
      return true;
    }
  }
  value = {};
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto it = list_.begin(); it != list_.end(); it++) {
    if (it->first == key) {
      it->second = value;  // update
      return true;
    }
  }
  if (IsFull()) {
    return false;  // full
  }
  list_.push_back({key, value});  // insert
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
