#include "db/compaction_state.h"
#include "db/column_family.h"

namespace rocksdb {

  SubcompactionState& SubcompactionState::operator=(SubcompactionState&& o) {
    compaction = std::move(o.compaction);
    selected_range_ = std::move(o.selected_range_);
    actual_start = std::move(o.actual_start);
    actual_end = std::move(o.actual_end);
    status = std::move(o.status);
    outputs = std::move(o.outputs);
    outfile = std::move(o.outfile);
    builder = std::move(o.builder);
    blob_outputs = std::move(o.blob_outputs);
    blob_outfile = std::move(o.blob_outfile);
    blob_builder = std::move(o.blob_builder);
    current_output_file_size = std::move(o.current_output_file_size);
    total_bytes = std::move(o.total_bytes);
    num_input_records = std::move(o.num_input_records);
    num_output_records = std::move(o.num_output_records);
    compaction_job_stats = std::move(o.compaction_job_stats);
    approx_size = std::move(o.approx_size);
    grandparent_index = std::move(o.grandparent_index);
    overlapped_bytes = std::move(o.overlapped_bytes);
    seen_key = std::move(o.seen_key);
    compression_dict = std::move(o.compression_dict);
    return *this;
  }

bool SubcompactionState::ShouldStopBefore(const Slice& internal_key,
                                          uint64_t curr_file_size) {
  const InternalKeyComparator* icmp =
      &compaction->column_family_data()->internal_comparator();
  const std::vector<FileMetaData*>& grandparents = compaction->grandparents();

  // Scan to find earliest grandparent file that contains key.
  while (grandparent_index < grandparents.size() &&
         icmp->Compare(internal_key,
                       grandparents[grandparent_index]->largest.Encode()) > 0) {
    if (seen_key) {
      overlapped_bytes += grandparents[grandparent_index]->fd.GetFileSize();
    }
    assert(grandparent_index + 1 >= grandparents.size() ||
           icmp->Compare(grandparents[grandparent_index]->largest,
                         grandparents[grandparent_index + 1]->smallest) <= 0);
    grandparent_index++;
  }
  seen_key = true;

  if (overlapped_bytes + curr_file_size > compaction->max_compaction_bytes()) {
    // Too much overlap for current output; start new output
    overlapped_bytes = 0;
    return true;
  }

  return false;
}
}  // namespace rocksdb