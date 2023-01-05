//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);  // make sure reset to invalid
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::HasKey(const KeyType &key, const KeyComparator &comparator) const -> bool {
  int l_index = 0;
  int r_index = GetSize() - 1;
  while (l_index <= r_index) {
    int m_index = l_index + (r_index - l_index) / 2;
    if (comparator(KeyAt(m_index), key) >= 0) {
      r_index = m_index - 1;
    } else {
      l_index = m_index + 1;
    }
  }
  // l_index is the lower_bound
  return IsIndexValid(l_index) && (comparator(KeyAt(l_index), key) == 0);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetKeyIndex(const KeyType &key, const KeyComparator &comparator) const -> int {
  int l_index = 0;
  int r_index = GetSize() - 1;
  while (l_index <= r_index) {
    int m_index = l_index + (r_index - l_index) / 2;
    if (comparator(KeyAt(m_index), key) >= 0) {
      r_index = m_index - 1;
    } else {
      l_index = m_index + 1;
    }
  }
  // l_index is the lower_bound
  if (IsIndexValid(l_index) && comparator(KeyAt(l_index), key) == 0) {
    return l_index;
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetElem(int index) -> const MappingType & { return array_[index]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator, ValueType &result) const
    -> bool {
  int l_index = 0;
  int r_index = GetSize() - 1;
  // do binary search
  while (l_index <= r_index) {
    int m_index = l_index + (r_index - l_index) / 2;
    if (comparator(KeyAt(m_index), key) >= 0) {
      r_index = m_index - 1;
    } else {
      l_index = m_index + 1;
    }
  }
  if (IsIndexValid(l_index) && comparator(KeyAt(l_index), key) == 0) {
    result = ValueAt(l_index);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertValue(const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> bool {
  int l_index = 0;
  int r_index = GetSize() - 1;
  while (l_index <= r_index) {
    int m_index = l_index + (r_index - l_index) / 2;
    if (comparator(KeyAt(m_index), key) <= 0) {
      l_index = m_index + 1;
    } else {
      r_index = m_index - 1;
    }
  }
  // l_index is the first position larger than key
  if (IsIndexValid(l_index) && comparator(KeyAt(l_index - 1), key) == 0) {
    return false;
    // duplicated key, fail to insert
  }
  for (int i = GetSize(); i >= l_index; i--) {
    swap(array_[i + 1], array_[i]);
  }
  array_[l_index] = std::make_pair(key, value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::InsertValueAndSplitTwo(const KeyType &key, const ValueType &value,
                                                        const KeyComparator &comparator,
                                                        BPlusTreeLeafPage &new_leaf_page) -> KeyType {
  // new_leaf_page.array_[0] = std::make_pair(key, value);
  // 0
  // 0 1
  // 0 1 2
  // 0 1 2 3
  // 0 1 2 3 4
  int current_size = GetSize();
  // left total_size/2  <->   right total_size-total_size/2
  int move_index = -1;
  bool to_right = true;
  if (current_size % 2 == 1) {
    // odd
    if (comparator(KeyAt(current_size / 2), key) < 0) {
      move_index = current_size / 2 + 1;
      to_right = true;
    } else {
      move_index = current_size / 2;
      to_right = false;
    }
  } else {
    // even
    if (comparator(KeyAt(current_size / 2 - 1), key) < 0) {
      move_index = current_size / 2;
      to_right = true;
    } else {
      move_index = current_size / 2 - 1;
      to_right = false;
    }
  }
  for (int i = move_index; i < current_size; i++) {
    swap(array_[i], new_leaf_page.array_[i - move_index]);
  }

  // for (int i = (current_size + 1) / 2; i < current_size; i++) {
  //   swap(array_[i], new_leaf_page.array_[i - (current_size + 1) / 2 + 1]);
  // }
  IncreaseSize(move_index - current_size);
  new_leaf_page.IncreaseSize(current_size - move_index);
  if (to_right) {
    new_leaf_page.InsertValue(key, value, comparator);
  } else {
    InsertValue(key, value, comparator);
  }
  return new_leaf_page.KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveValue(const KeyType &key, const KeyComparator &comparator) -> bool {
  // remove the key
  int l_index = 0;
  int r_index = GetSize() - 1;
  // do binary search
  while (l_index <= r_index) {
    int m_index = l_index + (r_index - l_index) / 2;
    if (comparator(KeyAt(m_index), key) >= 0) {
      r_index = m_index - 1;
    } else {
      l_index = m_index + 1;
    }
  }
  if (comparator(KeyAt(l_index), key) == 0) {
    array_[l_index].first = KeyType{};
    array_[l_index].second = ValueType{};
    for (int i = l_index; i < GetSize() - 1; i++) {
      swap(array_[i], array_[i + 1]);
    }
    IncreaseSize(-1);
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealFromLeft(BPlusTreeLeafPage &left_page) -> std::pair<page_id_t, KeyType> {
  page_id_t before_val = GetPageId();
  for (int i = GetSize() - 1; i >= 0; i--) {
    swap(array_[i], array_[i + 1]);  // move array_ right 1
  }
  swap(left_page.array_[left_page.GetSize() - 1], array_[0]);
  IncreaseSize(1);
  left_page.IncreaseSize(-1);
  KeyType after_key = KeyAt(0);
  return std::make_pair(before_val, after_key);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::StealFromRight(BPlusTreeLeafPage &right_page) -> std::pair<page_id_t, KeyType> {
  page_id_t before_val = right_page.GetPageId();
  swap(array_[GetSize()], right_page.array_[0]);
  for (int i = 0; i < right_page.GetSize() - 1; i++) {
    swap(right_page.array_[i], right_page.array_[i + 1]);
  }
  IncreaseSize(1);
  right_page.IncreaseSize(-1);
  KeyType after_key = right_page.KeyAt(0);
  return std::make_pair(before_val, after_key);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::MergeLeafPage(BPlusTreeLeafPage &tb_merged_page) -> page_id_t {
  // return the page_id should be deleted in the parent page
  for (int i = 0; i < tb_merged_page.GetSize(); i++) {
    swap(array_[GetSize() + i], tb_merged_page.array_[i]);
  }
  SetNextPageId(tb_merged_page.GetNextPageId());
  IncreaseSize(tb_merged_page.GetSize());
  tb_merged_page.SetSize(0);
  return tb_merged_page.GetPageId();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
