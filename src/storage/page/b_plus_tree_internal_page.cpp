//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReplaceKey(const ValueType &before_val, const KeyType &after_key,
                                                const KeyComparator &comparator) {
  for (int i = 1; i < GetSize(); i++) {
    if (before_val == ValueAt(i)) {
      SetKeyAt(i, after_key);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ReplaceKey(const KeyType &before_key, const KeyType &after_key,
                                                const KeyComparator &comparator) {
  for (int i = 1; i < GetSize(); i++) {
    if (comparator(KeyAt(i), before_key) == 0) {
      SetKeyAt(i, after_key);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetValue(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  int l_index1 = 1;
  int r_index1 = GetSize() - 1;
  while (l_index1 <= r_index1) {
    int m_index1 = l_index1 + (r_index1 - l_index1) / 2;
    if (comparator(KeyAt(m_index1), key) <= 0) {
      l_index1 = m_index1 + 1;
    } else {
      r_index1 = m_index1 - 1;
    }
  }
  return ValueAt(l_index1 - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKey(const KeyType &key, const ValueType &l_value, const ValueType &r_value,
                                               const KeyComparator &comparator) -> bool {
  // insert this structure
  // | ?????,l_value | key,r_value |

  // find the correct position

  // maybe still need to split (no ! this decision should be made before enter the func)

  // ValueType is page_id_t

  // find l_value
  // find the correct position (the first place larger than key)
  int l_index = 1;
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
  for (int i = GetSize(); i >= l_index; i--) {
    swap(array_[i + 1], array_[i]);
  }
  array_[l_index] = std::make_pair(key, r_value);
  // check
  if (GetSize() == 0) {
    // more a l_value;
    SetValueAt(l_index - 1, l_value);
    IncreaseSize(1);
  } else {
    BUSTUB_ASSERT(ValueAt(l_index - 1) == l_value, "ValueAt(l_index-1) should equal to the l_value");
  }

  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyIgnoreFirst(const KeyType &key, const ValueType &l_value,
                                                          const ValueType &r_value, const KeyComparator &comparator)
    -> bool {
  // find l_value
  // find the correct position (the first place larger than key)
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
  for (int i = GetSize(); i >= l_index; i--) {
    swap(array_[i + 1], array_[i]);
  }
  array_[l_index] = std::make_pair(key, r_value);
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertKeyAndSplitTwo(const KeyType &key, const ValueType &l_value,
                                                          const ValueType &r_value, const KeyComparator &comparator,
                                                          BPlusTreeInternalPage &new_internal_page) -> KeyType {
  // 这个部分最麻烦了，因为可能会递归的分裂，一直到root
  // 插入| ?????,l_value | key,r_value | 再分裂

  // 向当前internal page 插入一个 key，其左右分别为l_value,r_value
  // get the corrent m_key first

  // 0
  // 0 1
  // 0 1 2
  // 0 1 2 3
  // 0 1 2 3 4
  int current_size = GetSize();
  // left total_size/2  <->   right total_size-total_size/2
  int move_index = -1;
  bool to_right = true;

  if (comparator(KeyAt(current_size / 2), key) < 0) {
    move_index = current_size / 2 + 1;
    to_right = true;
  } else {
    move_index = current_size / 2;
    to_right = false;
  }

  // if ((current_size - 1) % 2 == 1) {
  //   // odd
  //   if (comparator(KeyAt(current_size / 2), key) < 0) {
  //     move_index = current_size / 2 + 1;
  //     to_right = true;
  //   } else {
  //     move_index = current_size / 2;
  //     to_right = false;
  //     // std::cout<<"in file: "<<__FILE__<<" in line: "<<__LINE__<<" KeyAt(current_size / 2): "<<KeyAt(current_size /
  //     // 2)<<std::endl;
  //   }
  // } else {
  //   // even
  //   if (comparator(KeyAt(current_size / 2), key) < 0) {
  //     move_index = current_size / 2 + 1;
  //     to_right = true;
  //   } else {
  //     move_index = current_size / 2;
  //     to_right = false;
  //   }
  // }

  // std::cout<<"in file: "<<__FILE__<<" in line: "<<__LINE__<<" to_right: "<<to_right<<std::endl;
  for (int i = move_index; i < current_size; i++) {
    swap(array_[i], new_internal_page.array_[i - move_index]);
  }
  IncreaseSize(move_index - current_size);
  new_internal_page.IncreaseSize(current_size - move_index);
  if (to_right) {
    new_internal_page.InsertKeyIgnoreFirst(key, l_value, r_value, comparator);
  } else {
    InsertKey(key, l_value, r_value, comparator);
  }

  // std::cout<<"in file: "<<__FILE__<<" in line: "<<__LINE__<<" key: "<<key<<std::endl;
  // std::cout<<"in file: "<<__FILE__<<" in line: "<<__LINE__<<" new_internal_page.KeyAt(0):
  // "<<new_internal_page.KeyAt(0)<<std::endl;
  KeyType ret = new_internal_page.KeyAt(0);  // attention this is 0
  new_internal_page.SetKeyAt(0, KeyType{});
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::GetSibling(const ValueType &value) const -> std::pair<page_id_t, page_id_t> {
  for (int i = 0; i < GetSize(); i++) {
    if (ValueAt(i) == value) {
      if (i == 0) {
        // no left only right
        return std::make_pair(INVALID_PAGE_ID, static_cast<page_id_t>(ValueAt(i + 1)));
      }
      if (i == GetSize() - 1) {
        // no right only left
        return std::make_pair(static_cast<page_id_t>(ValueAt(i - 1)), INVALID_PAGE_ID);
      }
      return std::make_pair(static_cast<page_id_t>(ValueAt(i - 1)), static_cast<page_id_t>(ValueAt(i + 1)));
    }
  }
  return std::make_pair(INVALID_PAGE_ID, INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveValue(const ValueType &value) -> bool {
  for (int i = 1; i < GetSize(); i++) {
    if (ValueAt(i) == value) {
      array_[i].first = KeyType{};
      array_[i].second = ValueType{};
      for (int j = i; j < GetSize() - 1; j++) {
        swap(array_[j], array_[j + 1]);
      }
      IncreaseSize(-1);
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromLeftAndUpdateParent(BPlusTreeInternalPage &left_page,
                                                                  BPlusTreeInternalPage &parent_page) -> page_id_t {
  // 这里主要实现从左侧internal_page偷取节点，需要注意的更改
  for (int i = 1; i < parent_page.GetSize(); i++) {
    if (parent_page.ValueAt(i) == GetPageId()) {
      for (int j = GetSize() - 1; j >= 0; j--) {
        swap(array_[j], array_[j + 1]);  // move array_ right 1
      }

      SetKeyAt(1, parent_page.KeyAt(i));
      SetValueAt(0, left_page.ValueAt(left_page.GetSize() - 1));
      parent_page.SetKeyAt(i, left_page.KeyAt(left_page.GetSize() - 1));

      left_page.array_[left_page.GetSize() - 1].first = KeyType{};
      left_page.array_[left_page.GetSize() - 1].second = ValueType{};

      IncreaseSize(1);
      left_page.IncreaseSize(-1);
      return ValueAt(0);
    }
  }
  BUSTUB_ASSERT(false, "internal_page steal error");
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::StealFromRightAndUpdateParent(BPlusTreeInternalPage &right_page,
                                                                   BPlusTreeInternalPage &parent_page) -> page_id_t {
  for (int i = 1; i < parent_page.GetSize(); i++) {
    if (parent_page.ValueAt(i) == right_page.GetPageId()) {
      SetKeyAt(GetSize(), parent_page.KeyAt(i));
      SetValueAt(GetSize(), right_page.ValueAt(0));
      parent_page.SetKeyAt(i, right_page.KeyAt(1));

      for (int j = 0; i < right_page.GetSize() - 1; j++) {
        swap(right_page.array_[j], right_page.array_[j + 1]);
      }

      right_page.array_[right_page.GetSize() - 1].first = KeyType{};
      right_page.array_[right_page.GetSize() - 1].second = ValueType{};

      right_page.SetKeyAt(0, KeyType{});  // reset the invalid key
      IncreaseSize(1);
      right_page.IncreaseSize(-1);
      return ValueAt(GetSize() - 1);
    }
  }
  BUSTUB_ASSERT(false, "internal_page steal error");
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::MergeInternalPage(BPlusTreeInternalPage &tb_merged_page,
                                                       BPlusTreeInternalPage &parent_page) -> page_id_t {
  // 将tb_merged_page 合并至 this
  // 需要注意的是，要把tb_merged_page的key0更新成新key，之后直接swap即可
  // 当然也不能忘记更新sub_page的parent_page_id，这个操作不在本函数进行
  for (int i = 0; i < parent_page.GetSize(); i++) {
    if (parent_page.ValueAt(i) == tb_merged_page.GetPageId()) {
      tb_merged_page.SetKeyAt(0, parent_page.KeyAt(i));
    }
  }
  for (int i = 0; i < tb_merged_page.GetSize(); i++) {
    swap(array_[GetSize() + i], tb_merged_page.array_[i]);
  }
  IncreaseSize(tb_merged_page.GetSize());
  tb_merged_page.SetSize(0);
  return tb_merged_page.GetPageId();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
