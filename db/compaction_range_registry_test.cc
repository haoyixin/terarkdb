//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction.h"

#include <string>

#include "util/random.h"
#include "util/testharness.h"
#include "rocksdb/comparator.h"

namespace rocksdb {

class RangeRegistryTest : public testing::Test {
 public:
  class MyComparator : public Comparator {
   public:
    int Compare(const Slice& a, const Slice& b) const override {
      uint64_t ia = std::atoi(a.data());
      uint64_t ib = std::atoi(b.data());

      return ia - ib;
    }

    const char* Name() const override { return "MyTestComparator"; }
    void FindShortestSeparator(std::string* start,
                               const Slice& limit) const override {}

    void FindShortSuccessor(std::string* key) const override {}
  };
  static SelectedRange Int64pairToSelectedRange(uint64_t start, uint64_t end,
                                                bool include_start,
                                                bool include_limit) {
    SelectedRange sr;
    sr.start = std::to_string(start);
    sr.limit = std::to_string(end);
    sr.include_start = include_start;
    sr.include_limit = include_limit;
    sr.is_infinite = false;
    return sr;
  }
  static const SelectedRange infinite_range() { return SelectedRange(true); }
};

TEST_F(RangeRegistryTest, Basic) {
  RangeRegistryTest::MyComparator mc;
  RangeRegistry rr(&mc);

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, true, false)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, true, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, true, true)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, false, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, false, true)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, false, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, false, false)));

  EXPECT_TRUE(rr.RegisterRange(infinite_range()));
  EXPECT_TRUE(rr.UnregisterRange(infinite_range()));
}

TEST_F(RangeRegistryTest, Multi) {
  RangeRegistryTest::MyComparator mc;
  RangeRegistry rr(&mc);

  // bigger range and smaller range, even empty range,  register and unregistered again
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(14, 19, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(24, 29, true, false)));
  EXPECT_FALSE(rr.RegisterRange(Int64pairToSelectedRange(11, 23, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(11, 13, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(11, 13, true, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(14, 19, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(14, 19, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(14, 19, true, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, true, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(24, 29, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(24, 29, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(24, 29, true, false)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 8, false, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 8, false, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 8, false, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 8, false, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 8, false, false)));

  // appressed ranges
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(10, 19, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 10, true, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(10, 19, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 10, true, false)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, false, true)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(10, 19, false, true)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 10, false, true)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, false, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(10, 19, false, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 10, false, true)));

  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, false, true)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(10, 19, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 10, false, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, false, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(10, 19, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 10, false, false)));

  // infinite range
  EXPECT_TRUE(rr.RegisterRange(infinite_range()));
  EXPECT_FALSE(rr.RegisterRange(Int64pairToSelectedRange(10, 19, true, false)));
  EXPECT_FALSE(rr.RegisterRange(infinite_range()));
  EXPECT_FALSE(rr.RegisterRange(Int64pairToSelectedRange(1, 9, true, false)));

  EXPECT_TRUE(rr.UnregisterRange(infinite_range()));

  // some random range
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 8, false, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 19, false, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(8, 8, true, true)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(19, 20, true, false)));
  EXPECT_TRUE(rr.RegisterRange(Int64pairToSelectedRange(0, 0, false, false)));

  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 8, false, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 19, false, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(8, 8, true, true)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(19, 20, true, false)));
  EXPECT_TRUE(rr.UnregisterRange(Int64pairToSelectedRange(0, 0, false, false)));


}
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
