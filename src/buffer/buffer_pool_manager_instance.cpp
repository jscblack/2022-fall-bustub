//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t spare_frame_id;
  if (!free_list_.empty()) {
    spare_frame_id = *free_list_.begin();
    free_list_.erase(free_list_.begin());
  } else {
    bool ret = replacer_->Evict(&spare_frame_id);
    if (!ret) {
      return nullptr;
    }
    // write back
    if (pages_[spare_frame_id].IsDirty()) {
      disk_manager_->WritePage(pages_[spare_frame_id].GetPageId(), pages_[spare_frame_id].GetData());
    }
    page_table_->Remove(pages_[spare_frame_id].GetPageId());
  }
  // use the spare_frame to create a new page
  // spare_frame to page_id
  page_id_t new_page = AllocatePage();
  *page_id = new_page;
  // rest the pages
  pages_[spare_frame_id].ResetMemory();
  pages_[spare_frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[spare_frame_id].pin_count_ = 0;
  pages_[spare_frame_id].is_dirty_ = false;
  // rest the pages
  pages_[spare_frame_id].page_id_ = new_page;
  pages_[spare_frame_id].pin_count_++;
  page_table_->Insert(new_page, spare_frame_id);
  replacer_->SetEvictable(spare_frame_id, false);
  replacer_->RecordAccess(spare_frame_id);
  return pages_ + spare_frame_id;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!page_table_->Find(page_id, frame_id)) {
    // not in buffer pool
    // try to get a available frame to contain the data
    frame_id_t spare_frame_id;
    if (!free_list_.empty()) {
      spare_frame_id = *free_list_.begin();
      free_list_.erase(free_list_.begin());
    } else {
      bool ret = replacer_->Evict(&spare_frame_id);
      if (!ret) {
        return nullptr;
      }
      // write back
      if (pages_[spare_frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[spare_frame_id].GetPageId(), pages_[spare_frame_id].GetData());
      }
      page_table_->Remove(pages_[spare_frame_id].GetPageId());
    }
    // rest the pages
    pages_[spare_frame_id].ResetMemory();
    pages_[spare_frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[spare_frame_id].pin_count_ = 0;
    pages_[spare_frame_id].is_dirty_ = false;
    // rest the pages
    pages_[spare_frame_id].pin_count_++;
    pages_[spare_frame_id].page_id_ = page_id;

    disk_manager_->ReadPage(page_id, pages_[spare_frame_id].data_);

    page_table_->Insert(page_id, spare_frame_id);
    replacer_->SetEvictable(spare_frame_id, false);
    replacer_->RecordAccess(spare_frame_id);
    return pages_ + spare_frame_id;
  }
  // in buffer pool
  pages_[frame_id].pin_count_++;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return pages_ + frame_id;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (is_dirty) {
      pages_[frame_id].is_dirty_ = is_dirty;
    }
    if (pages_[frame_id].GetPinCount() > 0) {
      pages_[frame_id].pin_count_--;
      if (pages_[frame_id].pin_count_ == 0) {
        replacer_->SetEvictable(frame_id, true);
      }
      return true;
    }
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    // std::cout << "FlushPgImp: "
    //           << "flushing page:" << page_id << " in frame:" << frame_id
    //           << std::endl;
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
    pages_[frame_id].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (page_table_->Find(page_id, frame_id)) {
    if (pages_[frame_id].GetPinCount() > 0) {
      return false;
    }
    // if (pages_[frame_id].IsDirty()) {
    //   disk_manager_->WritePage(pages_[frame_id].GetPageId(),
    //                            pages_[frame_id].GetData());
    // }
    pages_[frame_id].ResetMemory();
    pages_[frame_id].page_id_ = INVALID_PAGE_ID;
    pages_[frame_id].pin_count_ = 0;
    pages_[frame_id].is_dirty_ = false;
    replacer_->Remove(frame_id);
    free_list_.push_back(frame_id);
    DeallocatePage(page_id);
    return true;
  }

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
