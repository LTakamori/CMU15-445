//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  // free list的内容已经写死为o-i，作为map中的frame_id, 而page_id则可以任意（参考cache）
  // free list中记录的是没有被占用的frame_id, replacer 中记录的是 page_id, pages中存储的是每一个page的具体数据&meta data
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  std::unordered_map<page_id_t, frame_id_t>::iterator it;
  it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->Pin(page_id);
    pages_[it->second].pin_count_++;
    return pages_ + it->second;
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  frame_id_t replace_frame;
  if (!free_list_.empty()) {
    replace_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    page_id_t temp;
    replacer_->Victim(&temp);
    replace_frame = page_table_.find(temp) -> second;
  }

  // 2.     If R is dirty, write it back to the disk.
  if (pages_[replace_frame].IsDirty()) {
    disk_manager_->WritePage(pages_[replace_frame].page_id_, pages_[replace_frame].GetData());  // here !!! fuck u
  }

  // 3.     Delete R from the page table and insert P.
  page_table_.erase(pages_[replace_frame].page_id_);
  page_table_.emplace(page_id, replace_frame);

  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[replace_frame].page_id_ = page_id;
  pages_[replace_frame].is_dirty_ = false;
  pages_[replace_frame].pin_count_ = 1;
  replacer_->Pin(page_id);
  disk_manager_->ReadPage(page_id, pages_[replace_frame].data_);
  return pages_ + replace_frame;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  std::unordered_map<page_id_t, frame_id_t>::iterator it;
  it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  bool flag;
  flag = pages_[frame_id].GetPinCount() > 0;
  pages_[frame_id].is_dirty_ = is_dirty;
  pages_[frame_id].pin_count_--;  // todo: what if the pin count is negative?
  if (pages_[frame_id].pin_count_ <= 0) {
    pages_[frame_id].pin_count_ = 0;
    replacer_->Unpin(page_id);  // pin_count == 0才从LRU中将其标为unpin
  }
  return flag;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // Make sure you call DiskManager::WritePage!
  std::unordered_map<page_id_t, frame_id_t>::iterator it;
  it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // std::lock_guard<std::mutex> lock(latch_);
  // TODO: might have some dead lock issue here, i think, because the test freeze when i add lock here, modify later
  // 0.   Make sure you call DiskManager::AllocatePage!
  page_id_t temp_page_id = disk_manager_->AllocatePage();
  frame_id_t frame_id;
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    page_id_t temp;
    replacer_->Victim(&temp);  // victim 返回的是page id
    frame_id = page_table_.find(temp)->second;
    if (pages_[frame_id].is_dirty_) {
      FlushPageImpl(temp);
    }
    page_table_.erase(temp);
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  page_table_.emplace(temp_page_id, frame_id);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = temp_page_id;
  pages_[frame_id].pin_count_ = 1;  // todo 1 or 0?
  replacer_->Pin(temp_page_id);
  pages_[frame_id].is_dirty_ = false;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  *page_id = temp_page_id;
  return pages_ + frame_id;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  std::unordered_map<page_id_t, frame_id_t>::iterator it;
  it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  if (pages_[frame_id].pin_count_ > 0) {
    return false;
  }
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
 page_table_.erase(page_id);
 pages_[frame_id].page_id_ = INVALID_PAGE_ID;
 free_list_.emplace_back(static_cast<int>(frame_id));
 return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  std::lock_guard<std::mutex> lock(latch_);
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    page_id_t page_id = pages_[i].page_id_;
    if (page_id != INVALID_PAGE_ID) {
      // disk_manager_->WritePage(page_id, pages_[i].GetData());
      FlushPageImpl(page_id);
    }
  }
}

}  // namespace bustub
