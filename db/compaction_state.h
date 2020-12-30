#pragma once
#include "db/compaction.h"
#include "db/compaction_iterator.h"
#include "db/dbformat.h"

namespace rocksdb {

// Maintains state for each sub-compaction
struct SubcompactionState {
  const Compaction* compaction;
  std::unique_ptr<CompactionIterator> c_iter;

  // The boundaries of the key-range this compaction is interested in. No two
  // subcompactions may have overlapping key-ranges.
  // 'start' is inclusive, 'end' is exclusive, and nullptr means unbounded
  const Slice *start, *end;

  // actual range for this subcompaction
  InternalKey actual_start, actual_end;

  // The return status of this subcompaction
  Status status;

  // Files produced by this subcompaction
  struct Output {
    FileMetaData meta;

    // Same as user_collected_properties["User.Collected.Transient.Stat"].
    // Should be transient and do not save to SST, but the code is too
    // complex, so we DO SAVE it to SST, to avoid errors
    // std::string  stat_one;
    bool finished;
    std::shared_ptr<const TableProperties> table_properties;
  };
  std::string stat_all;

  // State kept for output being generated
  std::vector<Output> outputs;
  std::unique_ptr<WritableFileWriter> outfile;
  std::unique_ptr<TableBuilder> builder;
  Output* current_output() {
    if (outputs.empty()) {
      // This subcompaction's outptut could be empty if compaction was aborted
      // before this subcompaction had a chance to generate any output files.
      // When subcompactions are executed sequentially this is more likely and
      // will be particulalry likely for the later subcompactions to be empty.
      // Once they are run in parallel however it should be much rarer.
      return nullptr;
    } else {
      return &outputs.back();
    }
  }

  std::vector<Output> blob_outputs;
  std::unique_ptr<WritableFileWriter> blob_outfile;
  std::unique_ptr<TableBuilder> blob_builder;
  Output* current_blob_output() {
    if (blob_outputs.empty()) {
      return nullptr;
    } else {
      return &blob_outputs.back();
    }
  }

  uint64_t current_output_file_size;

  // State during the subcompaction
  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;
  CompactionJobStats compaction_job_stats;
  uint64_t approx_size;
  // An index that used to speed up ShouldStopBefore().
  size_t grandparent_index = 0;
  // The number of bytes overlapping between the current output and
  // grandparent files used in ShouldStopBefore().
  uint64_t overlapped_bytes = 0;
  // A flag determine whether the key has been seen in ShouldStopBefore()
  bool seen_key = false;
  std::string compression_dict;
  bool is_level_last_compaction = false;

  SubcompactionState(Compaction* c, const Slice _start, const Slice _end,
                     uint64_t size = 0, bool last_compact = false)
      : compaction(c),
        outfile(nullptr),
        builder(nullptr),
        current_output_file_size(0),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0),
        approx_size(size),
        grandparent_index(0),
        overlapped_bytes(0),
        seen_key(false),
        compression_dict(),
        is_level_last_compaction(last_compact),
        selected_range_(_start, _end, true, false),
        start_(selected_range_.start),
        end_(selected_range_.limit) {
    start = start_.empty() ? nullptr : &start_;
    end = end_.empty() ? nullptr : &end_;
    assert(compaction != nullptr);
  }

  SubcompactionState(SubcompactionState&& o) { *this = std::move(o); }

  SubcompactionState& operator=(SubcompactionState&& o);

  // Because member unique_ptrs do not have these.
  SubcompactionState(const SubcompactionState&) = delete;

  SubcompactionState& operator=(const SubcompactionState&) = delete;

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key, uint64_t curr_file_size);

 private:
  SelectedRange selected_range_;
  const Slice start_, end_;
};

// Maintains state for the entire compaction
struct CompactionState {
  Compaction* const compaction;

  // REQUIRED: subcompaction states are stored in order of increasing
  // key-range
  std::vector<SubcompactionState> sub_compact_states;
  Status status;

  uint64_t total_bytes;
  uint64_t num_input_records;
  uint64_t num_output_records;

  explicit CompactionState(Compaction* c)
      : compaction(c),
        total_bytes(0),
        num_input_records(0),
        num_output_records(0) {}

  size_t NumOutputFiles() {
    size_t total = 0;
    for (auto& s : sub_compact_states) {
      total += s.outputs.size();
      total += s.blob_outputs.size();
    }
    return total;
  }

  Slice SmallestUserKey() {
    for (const auto& sub_compact_state : sub_compact_states) {
      if (sub_compact_state.status.ok() && !sub_compact_state.outputs.empty() &&
          sub_compact_state.outputs[0].finished) {
        return sub_compact_state.outputs[0].meta.smallest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }

  Slice LargestUserKey() {
    for (auto it = sub_compact_states.rbegin(); it < sub_compact_states.rend();
         ++it) {
      if (it->status.ok() && !it->outputs.empty() &&
          it->current_output()->finished) {
        assert(it->current_output() != nullptr);
        return it->current_output()->meta.largest.user_key();
      }
    }
    // If there is no finished output, return an empty slice.
    return Slice(nullptr, 0);
  }
};

}  // namespace rocksdb
