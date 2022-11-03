/**
 * extendible_hash_test.cpp
 */

// #include <chrono>
#include "container/hash/extendible_hash_table.h"

#include <memory>
#include <random>
#include <thread>  // NOLINT
#include <unordered_map>

#include "gtest/gtest.h"
// // Generate n random strings
// std::vector<std::string> GenerateNRandomString(int n) {
//   std::random_device rd;
//   std::mt19937 gen(rd());
//   std::uniform_int_distribution<char> char_dist('A', 'z');
//   std::uniform_int_distribution<int> len_dist(1, 30);

//   std::vector<std::string> rand_strs(n);

//   for (auto &rand_str : rand_strs) {
//     int str_len = len_dist(gen);
//     for (int i = 0; i < str_len; ++i) {
//       rand_str.push_back(char_dist(gen));
//     }
//   }

//   return rand_strs;
// }
namespace bustub {

TEST(ExtendibleHashTableTest, SampleTest) {
  auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(2);

  table->Insert(1, "a");
  table->Insert(2, "b");
  table->Insert(3, "c");
  table->Insert(4, "d");
  table->Insert(5, "e");
  table->Insert(6, "f");
  table->Insert(7, "g");
  table->Insert(8, "h");
  table->Insert(9, "i");
  EXPECT_EQ(2, table->GetLocalDepth(0));
  EXPECT_EQ(3, table->GetLocalDepth(1));
  EXPECT_EQ(2, table->GetLocalDepth(2));
  EXPECT_EQ(2, table->GetLocalDepth(3));

  std::string result;
  table->Find(9, result);
  EXPECT_EQ("i", result);
  table->Find(8, result);
  EXPECT_EQ("h", result);
  table->Find(2, result);
  EXPECT_EQ("b", result);
  EXPECT_FALSE(table->Find(10, result));

  EXPECT_TRUE(table->Remove(8));
  EXPECT_TRUE(table->Remove(4));
  EXPECT_TRUE(table->Remove(1));
  EXPECT_FALSE(table->Remove(20));
}

TEST(ExtendibleHashTableTest, ConcurrentInsertTest) {
  const int num_runs = 50;
  const int num_threads = 3;

  // Run concurrent test multiple times to guarantee correctness.
  for (int run = 0; run < num_runs; run++) {
    auto table = std::make_unique<ExtendibleHashTable<int, int>>(2);
    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int tid = 0; tid < num_threads; tid++) {
      threads.emplace_back([tid, &table]() { table->Insert(tid, tid); });
    }
    for (int i = 0; i < num_threads; i++) {
      threads[i].join();
    }

    EXPECT_EQ(table->GetGlobalDepth(), 1);
    for (int i = 0; i < num_threads; i++) {
      int val;
      EXPECT_TRUE(table->Find(i, val));
      EXPECT_EQ(i, val);
    }
  }
}

// TEST(ExtendibleHashTableTest, DISABLED_SpeedTest) {
//   auto table = std::make_unique<ExtendibleHashTable<int, std::string>>(100);
//   std::unordered_map<int, std::string> stl_table;
//   int tot = 1e4;
//   std::vector<std::pair<int, std::string>> ve;
//   auto ret = GenerateNRandomString(tot);
//   for (int i = 0; i < tot; i++) {
//     ve.push_back({rand(), ret.at(i)});
//   }
//   typedef std::chrono::high_resolution_clock Clock;
//   auto t1 = Clock::now();
//   for (const auto &pr : ve) {
//     table->Insert(pr.first, pr.second);
//     // stl_table.insert({pr.first, pr.second});
//   }
//   auto t2 = Clock::now();
//   std::chrono::nanoseconds t21 = t2 - t1;
//   std::cout
//       << "time taken: "
//       << std::chrono::duration_cast<std::chrono::microseconds>(t21).count()
//       << " microseconds" << std::endl;
// }
}  // namespace bustub
