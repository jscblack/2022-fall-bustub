/**
 * index_iterator.cpp
 */
#include "storage/index/index_iterator.h"

#include <cassert>

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_ptr, int index,
                                  BufferPoolManager *buffer_pool_manager)
    : leaf_page_ptr_(leaf_page_ptr), index_(index), buffer_pool_manager_(buffer_pool_manager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_page_ptr_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_page_ptr_->GetPageId(), true);
    leaf_page_ptr_ = nullptr;
  }
};  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_page_ptr_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_page_ptr_->GetElem(index_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (index_ + 1 >= leaf_page_ptr_->GetSize()) {
    page_id_t nxt_page_id = leaf_page_ptr_->GetNextPageId();
    buffer_pool_manager_->UnpinPage(leaf_page_ptr_->GetPageId(), true);
    if (nxt_page_id == INVALID_PAGE_ID) {
      leaf_page_ptr_ = nullptr;
      index_ = 0;
    } else {
      leaf_page_ptr_ =
          reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(nxt_page_id)->GetData());
      index_ = 0;
    }
  } else {
    index_++;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
