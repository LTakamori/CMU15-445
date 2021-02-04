//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  current_index = 0;
  size = 0;
  capacity = num_pages;
}

LRUReplacer::~LRUReplacer() {
    for (auto p: pages) {
      delete p;
    }
};

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock(mutex);

  if (size == 0) {
    return false;
  }
  while(true) {
    if (pages[current_index]->pin_num != 0) {
      current_index = (current_index + 1) % pages.size();
      continue;
    }
    if (pages[current_index]->reference_bit) {
      pages[current_index]->reference_bit = false;
      current_index = (current_index + 1) % pages.size();
    } else {
      *frame_id = pages[current_index] -> page_id;
      pages.erase(pages.begin() + current_index);
      size--;
      if (size != 0) {
        current_index = current_index % size;
      } else {
        current_index = 0;
      }
      return true;
  }
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex);

  for (auto page : pages) {
    if (page -> page_id == frame_id) {
      page -> pin_num++;
      if (page -> pin_num == 1) {
        size--;
      }
      return;
    }
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(mutex);

  for (auto page : pages) {
    if (page -> page_id == frame_id) {
      if (page -> pin_num != 0) {
        page -> reference_bit = true;
        size++;
      }
      page -> pin_num = 0;
      return;
    }
  }

  if (size == capacity) {return;}  // TODO: raise exception
  PageNode* temp = new PageNode;
  temp -> reference_bit = false;
  temp -> page_id = frame_id;
  temp -> pin_num = 0;
  pages.push_back(temp);
  size++;
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(mutex);

  return size;
}

}  // namespace bustub
