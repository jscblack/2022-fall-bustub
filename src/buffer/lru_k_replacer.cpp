//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  frame_arr_.reserve(replacer_size_);
  for (size_t i = 0; i < num_frames; i++) {
    frame_arr_.emplace_back(Frame(k_));
  }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  size_t ps = SIZE_MAX;
  size_t longest_interval = 0;
  for (size_t i = 0; i < frame_arr_.size(); i++) {
    if (frame_arr_.at(i).IsInReplacer() && frame_arr_.at(i).GetEvictable()) {
      if (frame_arr_.at(i).GetLastKthAcc(current_timestamp_) == SIZE_MAX) {
        // this is not kth
        if (longest_interval == SIZE_MAX) {
          // muliple
          ps = SIZE_MAX;
          break;
        }
        ps = i;
        longest_interval = frame_arr_.at(i).GetLastKthAcc(current_timestamp_);
      } else if (frame_arr_.at(i).GetLastKthAcc(current_timestamp_) > longest_interval) {
        ps = i;
        longest_interval = frame_arr_.at(i).GetLastKthAcc(current_timestamp_);
      }
    }
  }
  if (ps != SIZE_MAX) {
    frame_arr_.at(ps).Evict();
    *frame_id = static_cast<frame_id_t>(ps);
    curr_size_--;
    return true;
  }
  // use classic lru
  longest_interval = 0;
  for (size_t i = 0; i < frame_arr_.size(); i++) {
    if (frame_arr_.at(i).IsInReplacer() && frame_arr_.at(i).GetEvictable() &&
        frame_arr_.at(i).GetLastKthAcc(current_timestamp_) == SIZE_MAX) {
      if (frame_arr_.at(i).GetLastAcc(current_timestamp_) > longest_interval) {
        ps = i;
        longest_interval = frame_arr_.at(i).GetLastAcc(current_timestamp_);
      }
    }
  }
  if (ps != SIZE_MAX) {
    frame_arr_.at(ps).Evict();
    *frame_id = static_cast<frame_id_t>(ps);
    curr_size_--;
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT((static_cast<size_t>(frame_id) > replacer_size_), "frame_id overflow");
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    return;
  }
  frame_arr_.at(frame_id).Access(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  // BUSTUB_ASSERT((static_cast<size_t>(frame_id) > replacer_size_), "frame_id overflow");
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    return;
  }
  if (!frame_arr_.at(frame_id).IsInReplacer()) {
    return;
  }
  if (frame_arr_.at(frame_id).GetEvictable() && !set_evictable) {
    curr_size_--;
    frame_arr_.at(frame_id).SetEvictable(set_evictable);
  } else if (!frame_arr_.at(frame_id).GetEvictable() && set_evictable) {
    curr_size_++;
    frame_arr_.at(frame_id).SetEvictable(set_evictable);
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (static_cast<size_t>(frame_id) >= replacer_size_) {
    return;
  }
  if (!frame_arr_.at(frame_id).IsInReplacer()) {
    return;
  }
  // BUSTUB_ASSERT((!frame_arr_.at(frame_id).GetEvictable()), "frame is not Evictable");
  if (!frame_arr_.at(frame_id).GetEvictable()) {
    return;
  }
  curr_size_--;
  frame_arr_.at(frame_id).Evict();
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

//========================//
LRUKReplacer::Frame::Frame(size_t size) : size_(size) {
  last_access_timestamp_.resize(size_);
  in_replacer_ = false;
  evictable_ = false;
  access_ptr_ = 0;
  for (auto &i : last_access_timestamp_) {
    i = SIZE_MAX;
  }
}
void LRUKReplacer::Frame::Evict() {
  in_replacer_ = false;
  evictable_ = false;
  access_ptr_ = 0;
  for (auto &i : last_access_timestamp_) {
    i = SIZE_MAX;
  }
}

void LRUKReplacer::Frame::Access(size_t timestamp) {
  if (!in_replacer_) {
    in_replacer_ = true;
  }
  last_access_timestamp_.at(access_ptr_) = timestamp;
  access_ptr_ = (access_ptr_ + 1) % size_;
}

auto LRUKReplacer::Frame::GetLastKthAcc(size_t timestamp) const -> size_t {
  if (last_access_timestamp_.at(access_ptr_) == SIZE_MAX) {
    return SIZE_MAX;
  }
  return timestamp - last_access_timestamp_.at(access_ptr_);
}
auto LRUKReplacer::Frame::GetLastAcc(size_t timestamp) const -> size_t {
  size_t target_ptr = access_ptr_;
  while (last_access_timestamp_.at(target_ptr) == SIZE_MAX) {
    target_ptr++;
    target_ptr %= size_;
  }
  return timestamp - last_access_timestamp_.at(target_ptr);
}

}  // namespace bustub
