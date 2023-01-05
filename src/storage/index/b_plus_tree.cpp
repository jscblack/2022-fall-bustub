#include "storage/index/b_plus_tree.h"

#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

/*
 * Helper function to get page id of the target leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPageId(const KeyType &key) -> page_id_t {
  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }
  auto *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (page_ptr != nullptr) {
    if (page_ptr->IsLeafPage()) {
      // is leaf page
      auto ret = page_ptr->GetPageId();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      return ret;
    }
    // is internal page
    auto *int_page_ptr = reinterpret_cast<InternalPage *>(page_ptr);

    page_id_t nxt_page_id = int_page_ptr->GetValue(key, comparator_);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
  }
  return INVALID_PAGE_ID;
}

/*
 * Helper function to get page id of the beginning leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetStartPageId() -> page_id_t {
  if (IsEmpty()) {
    return INVALID_PAGE_ID;
  }
  auto *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
  while (page_ptr != nullptr) {
    if (page_ptr->IsLeafPage()) {
      // is leaf page
      auto ret = page_ptr->GetPageId();
      buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
      return ret;
    }
    // is internal page
    auto *int_page_ptr = reinterpret_cast<InternalPage *>(page_ptr);
    page_id_t nxt_page_id = int_page_ptr->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
  }
  return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetSiblingPageId(const page_id_t &page_id) const -> std::pair<page_id_t, page_id_t> {
  auto *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
  if (page_ptr->IsRootPage()) {
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    return std::make_pair(INVALID_PAGE_ID, INVALID_PAGE_ID);
  }
  // any parent_page is internal_page
  auto *parent_page_ptr =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(page_ptr->GetParentPageId())->GetData());
  auto ret = parent_page_ptr->GetSibling(page_ptr->GetPageId());
  // std::cout<<"parent_page_ptr->GetSibling(page_ptr->GetPageId()):
  // "<<ret.first<<" "<<ret.second<<std::endl;
  buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
  return ret;
}

/*
 * Helper function to insert m_key to the internal page(do this recursively)
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertToInternalPageRecur(const KeyType &key, const page_id_t &l_value, const page_id_t &r_value,
                                               const page_id_t &internal_page_id) {
  // this function need to insert | ?????,l_value | key,r_value |
  // to the internal_page_id
  auto *internal_page_ptr =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());
  BUSTUB_ASSERT(internal_page_id == internal_page_ptr->GetPageId(),
                "internal_page_id should equal to the page id fetched");
  if (internal_page_ptr->GetSize() + 1 <= internal_page_ptr->GetMaxSize()) {
    // no need for split
    internal_page_ptr->InsertKey(key, l_value, r_value, comparator_);
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
  } else {
    // should split the internal page
    // and insert the m_key to the parent
    if (internal_page_ptr->IsRootPage()) {
      // create a new root page
      page_id_t new_root_page_id;
      Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_root_page_id);
      auto *new_root_page_ptr = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
      new_root_page_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
      root_page_id_ = new_root_page_ptr->GetPageId();
      UpdateRootPageId(false);
      internal_page_ptr->SetParentPageId(new_root_page_id);

      // create a new internal page
      page_id_t new_internal_page_id;
      new_page_ptr = buffer_pool_manager_->NewPage(&new_internal_page_id);
      auto *new_internal_page_ptr = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
      new_internal_page_ptr->Init(new_internal_page_id, internal_page_ptr->GetParentPageId(), internal_max_size_);

      // move the element
      KeyType m_key =
          internal_page_ptr->InsertKeyAndSplitTwo(key, l_value, r_value, comparator_, *new_internal_page_ptr);
      // std::cout<<"in file: "<<__FILE__<<" in line: "<<__LINE__<<" m_key: "<<m_key<<std::endl;
      for (int i = 0; i < new_internal_page_ptr->GetSize(); i++) {
        page_id_t nxt_page_id = new_internal_page_ptr->ValueAt(i);
        auto *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
        page_ptr->SetParentPageId(new_internal_page_ptr->GetPageId());
        buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      }
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_root_page_ptr->GetPageId(), true);
      InsertToInternalPageRecur(m_key, internal_page_id, new_internal_page_id, new_root_page_id);
    } else {
      // create a new internal page
      page_id_t new_internal_page_id;
      Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_internal_page_id);
      auto *new_internal_page_ptr = reinterpret_cast<InternalPage *>(new_page_ptr->GetData());
      new_internal_page_ptr->Init(new_internal_page_id, internal_page_ptr->GetParentPageId(), internal_max_size_);

      // move the element
      KeyType m_key =
          internal_page_ptr->InsertKeyAndSplitTwo(key, l_value, r_value, comparator_, *new_internal_page_ptr);
      for (int i = 0; i < new_internal_page_ptr->GetSize(); i++) {
        page_id_t nxt_page_id = new_internal_page_ptr->ValueAt(i);
        auto *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
        page_ptr->SetParentPageId(new_internal_page_id);
        buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
      }
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(new_internal_page_ptr->GetPageId(), true);
      InsertToInternalPageRecur(m_key, internal_page_id, new_internal_page_id, internal_page_ptr->GetParentPageId());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealFromSiblingLeafPage(const page_id_t &leaf_page_id) -> bool {
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());

  auto ret = GetSiblingPageId(leaf_page_id);
  page_id_t left_sib_leaf_page_id = ret.first;
  page_id_t right_sib_leaf_page_id = ret.second;
  if (left_sib_leaf_page_id != INVALID_PAGE_ID) {
    // steal from left
    // std::cout<<"steal from left"<<std::endl;
    auto *left_sib_leaf_page_ptr =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(left_sib_leaf_page_id)->GetData());
    if (left_sib_leaf_page_ptr->GetSize() - 1 >= left_sib_leaf_page_ptr->GetMinSize()) {
      // can be steal
      // std::cout<<"left can be steal"<<std::endl;
      auto replace_pair = leaf_page_ptr->StealFromLeft(*left_sib_leaf_page_ptr);
      // stolen update parent key
      auto *internal_page_ptr = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(leaf_page_ptr->GetParentPageId())->GetData());
      internal_page_ptr->ReplaceKey(replace_pair.first, replace_pair.second, comparator_);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sib_leaf_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(left_sib_leaf_page_ptr->GetPageId(),
                                    false);  // unchanged
  }
  if (right_sib_leaf_page_id != INVALID_PAGE_ID) {
    // steal from right
    // std::cout<<"steal from left"<<std::endl;
    auto *right_sib_leaf_page_ptr =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(right_sib_leaf_page_id)->GetData());
    if (right_sib_leaf_page_ptr->GetSize() - 1 >= right_sib_leaf_page_ptr->GetMinSize()) {
      // can be steal
      // std::cout<<"right can be steal"<<std::endl;
      auto replace_pair = leaf_page_ptr->StealFromRight(*right_sib_leaf_page_ptr);
      // stolen update parent key
      auto *internal_page_ptr = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(leaf_page_ptr->GetParentPageId())->GetData());
      internal_page_ptr->ReplaceKey(replace_pair.first, replace_pair.second, comparator_);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sib_leaf_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(right_sib_leaf_page_ptr->GetPageId(),
                                    false);  // unchanged
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(),
                                  false);  // unchanged
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::StealFromSiblingInternalPage(const page_id_t &internal_page_id) -> bool {
  auto *internal_page_ptr =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());
  auto ret = GetSiblingPageId(internal_page_id);
  page_id_t left_sib_internal_page_id = ret.first;
  page_id_t right_sib_internal_page_id = ret.second;
  if (left_sib_internal_page_id != INVALID_PAGE_ID) {
    // steal from left
    // std::cout<<"steal from left"<<std::endl;
    auto *left_sib_internal_page_ptr =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(left_sib_internal_page_id)->GetData());
    if (left_sib_internal_page_ptr->GetSize() - 1 >= std::max(2, left_sib_internal_page_ptr->GetMinSize())) {
      // 这里有一个点，应该是internal_page不能只剩一个，一个的时候意味着仅有一个pointer
      // can be steal
      // std::cout<<"left can be steal"<<std::endl;
      auto *parent_page_ptr = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(internal_page_ptr->GetParentPageId())->GetData());
      auto sub_page_id = internal_page_ptr->StealFromLeftAndUpdateParent(*left_sib_internal_page_ptr, *parent_page_ptr);
      // stolen update parent key
      auto *sub_page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sub_page_id)->GetData());
      sub_page_ptr->SetParentPageId(internal_page_ptr->GetPageId());

      buffer_pool_manager_->UnpinPage(sub_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(left_sib_internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(left_sib_internal_page_ptr->GetPageId(),
                                    false);  // unchanged
  }
  if (right_sib_internal_page_id != INVALID_PAGE_ID) {
    // steal from right
    // std::cout<<"steal from right"<<std::endl;
    auto *right_sib_internal_page_ptr =
        reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(right_sib_internal_page_id)->GetData());
    if (right_sib_internal_page_ptr->GetSize() - 1 >= std::max(2, right_sib_internal_page_ptr->GetMinSize())) {
      // 这里有一个点，应该是internal_page不能只剩一个，一个的时候意味着仅有一个pointer
      // can be steal
      // std::cout<<"right can be steal"<<std::endl;
      auto *parent_page_ptr = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(internal_page_ptr->GetParentPageId())->GetData());
      auto sub_page_id =
          internal_page_ptr->StealFromRightAndUpdateParent(*right_sib_internal_page_ptr, *parent_page_ptr);
      // stolen update parent key
      auto *sub_page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(sub_page_id)->GetData());
      sub_page_ptr->SetParentPageId(internal_page_ptr->GetPageId());

      buffer_pool_manager_->UnpinPage(sub_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(right_sib_internal_page_ptr->GetPageId(), true);
      buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
      return true;
    }
    buffer_pool_manager_->UnpinPage(right_sib_internal_page_ptr->GetPageId(),
                                    false);  // unchanged
  }
  buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(),
                                  false);  // unchanged
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteFromInternalPageRecur(const page_id_t &removed_page_id, page_id_t internal_page_id) {
  auto *internal_page_ptr =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(internal_page_id)->GetData());
  internal_page_ptr->RemoveValue(removed_page_id);

  if (internal_page_ptr->IsRootPage()) {
    // 如果是根节点，不受限制，需要额外操作
    bool need_to_change_root = (internal_page_ptr->GetSize() == 1);
    buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
    page_id_t tmp_zero = internal_page_ptr->ValueAt(0);
    if (need_to_change_root) {
      // 只剩一个pointer了，一个key都没有了
      // 删除这个page并且更新root_page
      root_page_id_ = tmp_zero;
      auto *tmp_root_page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(tmp_zero)->GetData());
      tmp_root_page_ptr->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(tmp_zero, true);
      UpdateRootPageId(false);
      auto check_here = buffer_pool_manager_->DeletePage(internal_page_id);
      BUSTUB_ASSERT(check_here == true, "the empty root page should not be pinned, and deleted successfully");
    }
    return;
  }

  if (internal_page_ptr->GetSize() < std::max(2, internal_page_ptr->GetMinSize())) {
    // 需要注意的一点是，如果internal_page的size为1时，即意味着所有的key都被删除了
    // 只剩一个pointer，此时也是不合法的
    // need further ops
    // 同样的，是尝试先偷，偷不成就合并
    auto stolen_from_sibling = StealFromSiblingInternalPage(internal_page_ptr->GetPageId());
    if (!stolen_from_sibling) {
      // merge recursively here
      std::cout << "merge internal page recursively here" << std::endl;
      // marked here (now pining page is internal_page_ptr)

      auto sib_page_id_pair = GetSiblingPageId(internal_page_ptr->GetPageId());
      BUSTUB_ASSERT(sib_page_id_pair.first != INVALID_PAGE_ID || sib_page_id_pair.second != INVALID_PAGE_ID,
                    "should have left or right sibling");
      auto tb_merged_internal_page_id = INVALID_PAGE_ID;
      InternalPage *tb_merged_internal_page_ptr = nullptr;
      if (sib_page_id_pair.first != INVALID_PAGE_ID) {
        // if have left sibling
        // merge the two
        tb_merged_internal_page_id = sib_page_id_pair.first;
        tb_merged_internal_page_ptr =
            reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(tb_merged_internal_page_id)->GetData());
        // notice always merge the right to left, so should do swap
        std::swap(internal_page_ptr, tb_merged_internal_page_ptr);
        std::swap(internal_page_id, tb_merged_internal_page_id);
      } else {
        // if have right sibling
        // merge the two
        tb_merged_internal_page_id = sib_page_id_pair.second;
        tb_merged_internal_page_ptr =
            reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(tb_merged_internal_page_id)->GetData());
      }
      BUSTUB_ASSERT(tb_merged_internal_page_ptr != nullptr, "tb_merged_internal_page_ptr should not be nullptr");

      // merge tb_merged_leaf_page to leaf_page
      // and delete tb_merged_leaf_page

      auto *parent_page_ptr = reinterpret_cast<InternalPage *>(
          buffer_pool_manager_->FetchPage(internal_page_ptr->GetParentPageId())->GetData());

      page_id_t to_be_removed = internal_page_ptr->MergeInternalPage(*tb_merged_internal_page_ptr, *parent_page_ptr);
      // 这个to_be_removed是parent_page的东西

      for (int i = internal_page_ptr->GetSize() - 1; i >= 0; i--) {
        auto *sub_page_ptr = reinterpret_cast<BPlusTreePage *>(
            buffer_pool_manager_->FetchPage(internal_page_ptr->ValueAt(i))->GetData());
        sub_page_ptr->SetParentPageId(internal_page_ptr->GetPageId());
        buffer_pool_manager_->UnpinPage(internal_page_ptr->ValueAt(i), false);
        if (internal_page_ptr->ValueAt(i) == to_be_removed) {
          break;
        }
      }

      BUSTUB_ASSERT(to_be_removed == tb_merged_internal_page_id,
                    "to_be_removed should be equal to tb_merged_leaf_page_id");
      buffer_pool_manager_->UnpinPage(tb_merged_internal_page_ptr->GetPageId(), true);

      // buffer_pool_manager_->DeletePage(tb_merged_leaf_page_ptr->GetPageId());
      BUSTUB_ASSERT(buffer_pool_manager_->DeletePage(tb_merged_internal_page_id) == true,
                    "the tb_merged_leaf_page should be deleted!!");
      // merge leaf page means to delete one leaf page, so need to delete a key
      // from the internal page, do this deletion recursively
      buffer_pool_manager_->UnpinPage(parent_page_ptr->GetPageId(), true);
      DeleteFromInternalPageRecur(to_be_removed, internal_page_ptr->GetParentPageId());
    }
  }
  buffer_pool_manager_->UnpinPage(internal_page_ptr->GetPageId(), true);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    return false;
  }
  auto leaf_page_id = GetLeafPageId(key);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  ValueType res;
  if (leaf_page_ptr->GetValue(key, comparator_, res)) {
    result->push_back(res);
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    page_id_t new_root_page_id;
    Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto *new_root_page_ptr =
        reinterpret_cast<LeafPage *>(new_page_ptr->GetData());  // the new root page is a leaf node
    new_root_page_ptr->Init(new_root_page_id, INVALID_PAGE_ID, leaf_max_size_);
    root_page_id_ = new_root_page_ptr->GetPageId();
    UpdateRootPageId(true);
    auto ret = new_root_page_ptr->InsertValue(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    return ret;
  }
  auto leaf_page_id = GetLeafPageId(key);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return false;  // error occurred
  }
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());

  BUSTUB_ASSERT(leaf_page_id == leaf_page_ptr->GetPageId(), "leaf_page_id should equal to the page id fetched");

  if (leaf_page_ptr->HasKey(key, comparator_)) {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(),
                                    false);  // flush non-dirty page
    return false;                            // duplicated key
  }
  // maybe need to split
  if (leaf_page_ptr->GetSize() + 1 < leaf_page_ptr->GetMaxSize()) {
    // no need for split
    auto ret = leaf_page_ptr->InsertValue(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(),
                                    true);  // flush dirty page
    return ret;
  }
  // page is full need to split
  page_id_t new_leaf_page_id;
  Page *new_page_ptr = buffer_pool_manager_->NewPage(&new_leaf_page_id);

  auto *new_leaf_page_ptr = reinterpret_cast<LeafPage *>(new_page_ptr->GetData());  // the new leaf page
  new_leaf_page_ptr->Init(new_leaf_page_id, leaf_page_ptr->GetParentPageId(), leaf_max_size_);
  leaf_page_ptr->SetNextPageId(new_leaf_page_ptr->GetPageId());  // link the leaf page
  // till now the new leaf page is created
  KeyType m_key = leaf_page_ptr->InsertValueAndSplitTwo(key, value, comparator_, *new_leaf_page_ptr);
  // find the middle key
  if (leaf_page_ptr->IsRootPage()) {
    // leaf_page is the root page
    // after split need a new root internal page
    page_id_t new_root_page_id;
    new_page_ptr = buffer_pool_manager_->NewPage(&new_root_page_id);
    auto *new_root_page_ptr =
        reinterpret_cast<InternalPage *>(new_page_ptr->GetData());  // the new root page is a internal node
    new_root_page_ptr->Init(new_root_page_id, INVALID_PAGE_ID, internal_max_size_);
    root_page_id_ = new_root_page_ptr->GetPageId();
    UpdateRootPageId(false);
    leaf_page_ptr->SetParentPageId(new_root_page_ptr->GetPageId());
    new_leaf_page_ptr->SetParentPageId(new_root_page_ptr->GetPageId());
    // now just insert to the parent_page
    new_root_page_ptr->InsertKey(m_key, leaf_page_id, new_leaf_page_id, comparator_);
    buffer_pool_manager_->UnpinPage(new_root_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
  } else {
    // leaf_page is not root page
    // after split just insert m_key to the parent_page
    page_id_t parent_page_id = leaf_page_ptr->GetParentPageId();
    buffer_pool_manager_->UnpinPage(new_leaf_page_ptr->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
    InsertToInternalPageRecur(m_key, leaf_page_id, new_leaf_page_id, parent_page_id);
  }

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  // todo remove here
  if (IsEmpty()) {
    return;
  }
  auto leaf_page_id = GetLeafPageId(key);
  if (leaf_page_id == INVALID_PAGE_ID) {
    return;  // error occurred
  }
  auto *leaf_page_ptr = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leaf_page_id)->GetData());
  if (!leaf_page_ptr->HasKey(key, comparator_)) {
    return;  // no such a key
  }
  // delete start here
  leaf_page_ptr->RemoveValue(key, comparator_);
  // 依然沿用insert的思路就是先调整好leaf_page，之后递归的去处理需要处理的internal_page

  if (leaf_page_ptr->IsRootPage()) {
    // 如果这是空的根节点，需要做删除操作，需要单独处理一下
    bool need_to_clear_tree = (leaf_page_ptr->GetSize() == 0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    if (need_to_clear_tree) {
      // delete this page
      BUSTUB_ASSERT(root_page_id_ == leaf_page_id, "the root leaf page should be root and leaf");
      auto check_here = buffer_pool_manager_->DeletePage(root_page_id_);
      BUSTUB_ASSERT(check_here == true, "the empty root page should not be pinned, and deleted successfully");
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
    }
    return;
  }

  // 非根叶节点，继续

  if (leaf_page_ptr->GetSize() < leaf_page_ptr->GetMinSize()) {
    // stage1:
    // try to steal one k-v pair from the left/right sibling
    auto stolen_from_sibling = StealFromSiblingLeafPage(leaf_page_ptr->GetPageId());
    if (!stolen_from_sibling) {
      // steal failed
      // need for stage2

      // stage2:
      // failed to steal, need merge the leaf page
      std::cout << "into stage2" << std::endl;

      // marked here (now pining page is leaf_page_ptr)
      auto sib_page_id_pair = GetSiblingPageId(leaf_page_ptr->GetPageId());
      BUSTUB_ASSERT(sib_page_id_pair.first != INVALID_PAGE_ID || sib_page_id_pair.second != INVALID_PAGE_ID,
                    "should have left or right sibling");
      auto tb_merged_leaf_page_id = INVALID_PAGE_ID;
      LeafPage *tb_merged_leaf_page_ptr = nullptr;
      if (sib_page_id_pair.first != INVALID_PAGE_ID) {
        // if have left sibling
        // merge the two
        tb_merged_leaf_page_id = sib_page_id_pair.first;
        tb_merged_leaf_page_ptr =
            reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(tb_merged_leaf_page_id)->GetData());
        // notice always merge the right to left, so should do swap
        std::swap(leaf_page_ptr, tb_merged_leaf_page_ptr);
        std::swap(leaf_page_id, tb_merged_leaf_page_id);
      } else {
        // if have right sibling
        // merge the two
        tb_merged_leaf_page_id = sib_page_id_pair.second;
        tb_merged_leaf_page_ptr =
            reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(tb_merged_leaf_page_id)->GetData());
      }
      BUSTUB_ASSERT(tb_merged_leaf_page_ptr != nullptr, "tb_merged_leaf_page_ptr should not be nullptr");

      // merge tb_merged_leaf_page to leaf_page
      // and delete tb_merged_leaf_page
      page_id_t to_be_removed = leaf_page_ptr->MergeLeafPage(*tb_merged_leaf_page_ptr);
      BUSTUB_ASSERT(to_be_removed == tb_merged_leaf_page_id, "to_be_removed should be equal to tb_merged_leaf_page_id");
      buffer_pool_manager_->UnpinPage(tb_merged_leaf_page_ptr->GetPageId(), true);

      // buffer_pool_manager_->DeletePage(tb_merged_leaf_page_ptr->GetPageId());
      BUSTUB_ASSERT(buffer_pool_manager_->DeletePage(tb_merged_leaf_page_id) == true,
                    "the tb_merged_leaf_page should be deleted!!");
      // merge leaf page means to delete one leaf page, so need to delete a key
      // from the internal page, do this deletion recursively

      // 从这边开始就是递归的从internal_page中开始删就行
      DeleteFromInternalPageRecur(to_be_removed, leaf_page_ptr->GetParentPageId());
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page_ptr->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  auto start_leaf_page_id = GetStartPageId();
  if (start_leaf_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  auto *start_leaf_page_ptr =
      reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(start_leaf_page_id)->GetData());
  return INDEXITERATOR_TYPE(start_leaf_page_ptr, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto start_leaf_page_id = GetLeafPageId(key);
  if (start_leaf_page_id == INVALID_PAGE_ID) {
    return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_);
  }
  auto *start_leaf_page_ptr =
      reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(start_leaf_page_id)->GetData());

  auto idx = start_leaf_page_ptr->GetKeyIndex(key, comparator_);
  if (idx == -1) {
    // not found
    return Begin();
  }
  return INDEXITERATOR_TYPE(start_leaf_page_ptr, idx, buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(nullptr, 0, buffer_pool_manager_); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (root_page_id_ == INVALID_PAGE_ID) {
    // the root page has gone
    header_page->DeleteRecord(index_name_);
  } else {
    if (insert_record != 0) {
      // create a new record<index_name + root_page_id> in header_page
      header_page->InsertRecord(index_name_, root_page_id_);
    } else {
      // update root_page_id in header_page
      header_page->UpdateRecord(index_name_, root_page_id_);
    }
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" "
           "CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
