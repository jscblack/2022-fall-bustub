//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <utility>
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);
  void ReplaceKey(const ValueType &before_val, const KeyType &after_key, const KeyComparator &comparator);
  void ReplaceKey(const KeyType &before_key, const KeyType &after_key, const KeyComparator &comparator);

  auto GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType;
  auto InsertKey(const KeyType &key, const ValueType &l_value, const ValueType &r_value,
                 const KeyComparator &comparator) -> bool;
  auto InsertKeyAndSplitTwo(const KeyType &key, const ValueType &l_value, const ValueType &r_value,
                            const KeyComparator &comparator, BPlusTreeInternalPage &new_internal_page) -> KeyType;
  auto GetSibling(const ValueType &value) const -> std::pair<page_id_t, page_id_t>;
  auto RemoveValue(const ValueType &value) -> bool;
  auto StealFromLeftAndUpdateParent(BPlusTreeInternalPage &left_page, BPlusTreeInternalPage &parent_page) -> page_id_t;
  auto StealFromRightAndUpdateParent(BPlusTreeInternalPage &right_page, BPlusTreeInternalPage &parent_page)
      -> page_id_t;
  auto MergeInternalPage(BPlusTreeInternalPage &tb_merged_page, BPlusTreeInternalPage &parent_page) -> page_id_t;

 private:
  auto InsertKeyIgnoreFirst(const KeyType &key, const ValueType &l_value, const ValueType &r_value,
                            const KeyComparator &comparator) -> bool;
  // Flexible array member for page data.
  MappingType array_[1];
};
}  // namespace bustub
