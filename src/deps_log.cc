// Copyright 2012 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "deps_log.h"

#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#elif defined(_MSC_VER) && (_MSC_VER < 1900)
typedef __int32 int32_t;
typedef unsigned __int32 uint32_t;
#endif

#include <numeric>

#include "disk_interface.h"
#include "graph.h"
#include "metrics.h"
#include "parallel_map.h"
#include "state.h"
#include "util.h"

// The version is stored as 4 bytes after the signature and also serves as a
// byte order mark. Signature and version combined are 16 bytes long.
static constexpr StringPiece kFileSignature { "# ninjadeps\n", 12 };
static_assert(kFileSignature.size() % 4 == 0,
              "file signature size is not a multiple of 4");
static constexpr size_t kFileHeaderSize = kFileSignature.size() + 4;
const int kCurrentVersion = 4;

// Record size is currently limited to less than the full 32 bit, due to
// internal buffers having to have this size.
const unsigned kMaxRecordSize = (1 << 19) - 1;

DepsLog::~DepsLog() {
  Close();
}

bool DepsLog::OpenForWrite(const string& path, const DiskInterface& disk, string* err) {
  if (needs_recompaction_) {
    if (!Recompact(path, disk, err))
      return false;
  }
  
  file_ = fopen(path.c_str(), "ab");
  if (!file_) {
    *err = strerror(errno);
    return false;
  }
  // Set the buffer size to this and flush the file buffer after every record
  // to make sure records aren't written partially.
  setvbuf(file_, NULL, _IOFBF, kMaxRecordSize + 1);
  SetCloseOnExec(fileno(file_));

  // Opening a file in append mode doesn't set the file pointer to the file's
  // end on Windows. Do that explicitly.
  fseek(file_, 0, SEEK_END);

  if (ftell(file_) == 0) {
    if (fwrite(kFileSignature.data(), kFileSignature.size(), 1, file_) < 1) {
      *err = strerror(errno);
      return false;
    }
    if (fwrite(&kCurrentVersion, 4, 1, file_) < 1) {
      *err = strerror(errno);
      return false;
    }
  }
  if (fflush(file_) != 0) {
    *err = strerror(errno);
    return false;
  }
  return true;
}

bool DepsLog::RecordDeps(Node* node, TimeStamp mtime,
                         const vector<Node*>& nodes) {
  return RecordDeps(node, mtime, nodes.size(),
                    nodes.empty() ? NULL : (Node**)&nodes.front());
}

bool DepsLog::RecordDeps(Node* node, TimeStamp mtime,
                         int node_count, Node** nodes) {
  // Track whether there's any new data to be recorded.
  bool made_change = false;

  // Assign ids to all nodes that are missing one.
  if (node->id() < 0) {
    if (!RecordId(node))
      return false;
    made_change = true;
  }
  for (int i = 0; i < node_count; ++i) {
    if (nodes[i]->id() < 0) {
      if (!RecordId(nodes[i]))
        return false;
      made_change = true;
    }
  }

  // See if the new data is different than the existing data, if any.
  if (!made_change) {
    Deps* deps = GetDeps(node);
    if (!deps ||
        deps->mtime != mtime ||
        deps->node_count != node_count) {
      made_change = true;
    } else {
      for (int i = 0; i < node_count; ++i) {
        if (deps->nodes[i] != nodes[i]) {
          made_change = true;
          break;
        }
      }
    }
  }

  // Don't write anything if there's no new info.
  if (!made_change)
    return true;

  // Update on-disk representation.
  unsigned size = 4 * (1 + 2 + node_count);
  if (size > kMaxRecordSize) {
    errno = ERANGE;
    return false;
  }
  size |= 0x80000000;  // Deps record: set high bit.
  if (fwrite(&size, 4, 1, file_) < 1)
    return false;
  int id = node->id();
  if (fwrite(&id, 4, 1, file_) < 1)
    return false;
  uint32_t mtime_part = static_cast<uint32_t>(mtime & 0xffffffff);
  if (fwrite(&mtime_part, 4, 1, file_) < 1)
    return false;
  mtime_part = static_cast<uint32_t>((mtime >> 32) & 0xffffffff);
  if (fwrite(&mtime_part, 4, 1, file_) < 1)
    return false;
  for (int i = 0; i < node_count; ++i) {
    id = nodes[i]->id();
    if (fwrite(&id, 4, 1, file_) < 1)
      return false;
  }
  if (fflush(file_) != 0)
    return false;

  // Update in-memory representation.
  Deps* deps = new Deps(mtime, node_count);
  for (int i = 0; i < node_count; ++i)
    deps->nodes[i] = nodes[i];
  UpdateDeps(node->id(), deps);

  return true;
}

void DepsLog::Close() {
  if (file_)
    fclose(file_);
  file_ = NULL;
}

// Return the number of words in the record, including the header, or 0 if
// the header is invalid.
static inline size_t RecordSizeInWords(size_t header) {
  header &= 0x7FFFFFFF;
  if (header % sizeof(uint32_t) != 0) return 0;
  // Either (node ID and mtime) or (data and checksum)
  if (header < sizeof(uint32_t) * 2) return 0;
  if (header > kMaxRecordSize) return 0;
  return header / sizeof(uint32_t) + 1;
}

static inline bool IsDepsRecordHeader(size_t header) {
  return (header & 0x80000000) == 0x80000000;
}

static inline bool IsValidDepsRecordHeader(size_t header) {
  return IsDepsRecordHeader(header) && RecordSizeInWords(header);
}

/// Split the v4 deps log into independently-parseable chunks using a heuristic.
/// If the heuristic fails, we'll still load the file correctly, but it could be
/// slower.
///
/// There are two kinds of records -- path and deps records. Their formats:
///
///    path:
///     - uint32 size -- high bit is clear
///     - String content. The string is padded to a multiple of 4 bytes with
///       trailing NULs.
///     - uint32 checksum (ones complement of the path's index / node ID)
///
///    deps:
///     - uint32 size -- high bit is set
///     - int32 output_path_id
///     - uint64 output_path_mtime
///     - int32 input_path_id[...] -- every remaining word is an input ID
///
/// To split the deps log into chunks, look for uint32 words with the value
/// 0x8000xxxx, where xxxx is nonzero. Such a word is almost guaranteed to be
/// the size field of a deps record (with fewer than ~16K dependencies):
///  - It can't be part of a string, because paths can't have embedded NULs.
///  - It (probably) can't be a node ID, because node IDs are represented using
///    "int", and it would be unlikely to have more than 2 billion of them. An
///    Android build typically has about 1 million nodes.
///  - It's unlikely to be part of a path checksum, because that would also
///    imply that we have at least 2 billion nodes.
///  - It could be the upper word of a mtime from 2262, or the lower word, which
///    wraps every ~4s. We rule these out by looking for the mtime's deps size
///    two or three words above the split candidate.
///
/// This heuristic can fail in a few ways:
///  - We only find path records in the area we scan.
///  - The deps records all have >16K of dependencies. (Almost all deps records
///    I've seen in the Android build have a few hundred. Only a few have ~10K.)
///  - All deps records with <16K of dependencies are preceded by something that
///    the mtime check rejects:
///     - A deps record with an mtime within a 524us span every 4s, with one
///       dependency.
///     - A deps record with an mtime from 2262, with one or two dependencies.
///     - A path record containing "xx xx 0[0-7] 80" (little-endian) or "80 0[0-7] xx xx"
///       (big-endian) as the last or second-to-last word. The "0[0-7]" is a
///       control character, and "0[0-7] 80" is invalid UTF-8.
///
/// Maybe we can add a delimiter to the log format and replace this code.
static std::vector<std::pair<size_t, size_t>>
SplitDepsLog(const uint32_t* table, size_t size, ThreadPool* thread_pool) {
  if (size == 0) return {};

  std::vector<std::pair<size_t, size_t>> blind_splits = SplitByThreads(size);
  std::vector<std::pair<size_t, size_t>> chunks;
  size_t chunk_start = 0;

  auto split_candidates = ParallelMap(thread_pool, blind_splits,
      [table](std::pair<size_t, size_t> chunk) {
    // Skip the first two words to allow for the mtime check later on.
    for (size_t index = chunk.first + 3; index < chunk.second; ++index) {
      size_t this_header = table[index];
      if (!IsValidDepsRecordHeader(this_header)) continue;
      if ((this_header & 0xFFFF0000) != 0x80000000) continue;

      // We've either found a deps record or a mtime. If it's an mtime, the
      // word two or three spaces back will be a valid deps size (0x800xxxxx).
      if (IsValidDepsRecordHeader(table[index - 3])) continue;
      if (IsValidDepsRecordHeader(table[index - 2])) continue;

      // Success: In a valid deps log, this index must start a deps record.
      return index;
    }
    return SIZE_MAX;
  });
  for (size_t candidate : split_candidates) {
    if (candidate != SIZE_MAX) {
      assert(chunk_start < candidate);
      chunks.push_back({ chunk_start, candidate });
      chunk_start = candidate;
    }
  }

  assert(chunk_start < size);
  chunks.push_back({ chunk_start, size });
  return chunks;
}

struct DepsLogInput {
  std::unique_ptr<LoadedFile> file;
  const uint32_t* table = nullptr;
  size_t table_size = 0;
};

static bool OpenDepsLogForReading(const std::string& path,
                                  DepsLogInput* log,
                                  std::string* err) {
  *log = {};

  RealDiskInterface file_reader;
  std::string load_err;
  switch (file_reader.LoadFile(path, &log->file, &load_err)) {
  case FileReader::Okay:
    break;
  case FileReader::NotFound:
    return true;
  default:
    *err = load_err;
    return false;
  }

  bool valid_header = false;
  int version = 0;
  if (log->file->content().size() >= kFileHeaderSize ||
      log->file->content().substr(0, kFileSignature.size()) == kFileSignature) {
    valid_header = true;
    memcpy(&version,
           log->file->content().data() + kFileSignature.size(),
           sizeof(version));
  }

  // Note: For version differences, this should migrate to the new format.
  // But the v1 format could sometimes (rarely) end up with invalid data, so
  // don't migrate v1 to v3 to force a rebuild. (v2 only existed for a few days,
  // and there was no release with it, so pretend that it never happened.)
  if (!valid_header || version != kCurrentVersion) {
    if (version == 1)
      *err = "deps log version change; rebuilding";
    else
      *err = "bad deps log signature or version; starting over";
    log->file.reset();
    unlink(path.c_str());
    // Don't report this as a failure.  An empty deps log will cause
    // us to rebuild the outputs anyway.
    return true;
  }

  log->table =
      reinterpret_cast<const uint32_t*>(
          log->file->content().data() + kFileHeaderSize);
  log->table_size =
      (log->file->content().size() - kFileHeaderSize) / sizeof(uint32_t);

  return true;
}

bool DepsLog::Load(const string& path, State* state, string* err) {
  METRIC_RECORD(".ninja_deps load");

  assert(nodes_.empty());
  DepsLogInput log;

  if (!OpenDepsLogForReading(path, &log, err)) return false;
  if (log.file.get() == nullptr) return true;

  struct NINJA_ALIGNAS_CACHE_LINE Chunk {
    size_t start = 0;
    size_t stop = 0;
    int first_node_id = 0;
    int initial_node_count = 0;
    int final_node_count = 0;
    size_t deps_count = 0;
    bool parse_error = false;
  };

  std::unique_ptr<ThreadPool> thread_pool = CreateThreadPool();

  std::vector<Chunk> chunks;
  for (std::pair<size_t, size_t> span :
      SplitDepsLog(log.table, log.table_size, thread_pool.get())) {
    Chunk chunk {};
    chunk.start = span.first;
    chunk.stop = span.second;
    chunks.push_back(chunk);
  }

  // Compute the starting node ID for each chunk. The result is correct as long as
  // preceding chunks are parsed successfully. If there is a parsing error in a
  // chunk, then following chunks are discarded after the validation pass.
  ParallelMap(thread_pool.get(), chunks, [&log](Chunk& chunk) {
    size_t index = chunk.start;
    while (index < chunk.stop) {
      size_t header = log.table[index];
      size_t size = RecordSizeInWords(header);
      if (!size) return; // invalid header
      if (!IsDepsRecordHeader(header)) {
        ++chunk.initial_node_count;
      }
      index += size;
    }
  });
  int initial_node_count = 0;
  for (size_t i = 0; i < chunks.size(); ++i) {
    Chunk& chunk = chunks[i];
    chunk.first_node_id = initial_node_count;
    initial_node_count += chunk.initial_node_count;
  }

  // A map from a node ID to the final file index of the deps record outputting
  // the given node ID.
  std::vector<std::atomic<ssize_t>> dep_index(initial_node_count);
  for (auto& index : dep_index) {
    // Write a value of -1 to indicate that no deps record outputs this ID. We
    // don't need these stores to be synchronized with other threads, so use
    // relaxed stores, which are much faster.
    index.store(-1, std::memory_order_relaxed);
  }
  // A map from a node ID to the file index of that node.
  std::vector<size_t> node_index(initial_node_count);

  // The main parsing pass. Validate each chunk's entries and, for each node ID,
  // record the location of its node and deps records. If there is parser error,
  // truncate the log just before the problem record.
  ParallelMap(thread_pool.get(), chunks,
      [&log, &dep_index, &node_index](Chunk& chunk) {
    size_t index = chunk.start;
    int next_node_id = chunk.first_node_id;
    while (index < chunk.stop) {
      size_t header = log.table[index];
      size_t size = RecordSizeInWords(header);
      if (!size || (index + size > chunk.stop)) break;
      if (IsDepsRecordHeader(header)) {
        // Verify that input/output node IDs are valid.
        int output_id = log.table[index + 1];
        if (output_id < 0 || output_id >= next_node_id) break;
        for (size_t i = 4; i < size; ++i) {
          int input_id = log.table[index + i];
          if (input_id < 0 || input_id >= next_node_id) break;
        }
        AtomicUpdateMaximum(&dep_index[output_id], static_cast<ssize_t>(index));
        ++chunk.deps_count;
      } else {
        // Validate the path's checksum.
        int checksum = log.table[index + size - 1];
        if (checksum != ~next_node_id) break;
        node_index[next_node_id] = index;
        ++next_node_id;
        ++chunk.final_node_count;
      }
      index += size;
    }
    // We'll exit early on a parser error.
    if (index < chunk.stop) {
      chunk.stop = index;
      chunk.parse_error = true;
    }
  });
  int node_count = 0;
  size_t total_dep_record_count = 0;
  for (size_t i = 0; i < chunks.size(); ++i) {
    Chunk& chunk = chunks[i];
    assert(chunk.first_node_id == node_count);
    total_dep_record_count += chunk.deps_count;
    node_count += chunk.final_node_count;
    if (chunk.parse_error) {
      // Part of this chunk may have been parsed successfully, so keep it, but
      // discard all later chunks.
      chunks.resize(i + 1);
      break;
    }
  }

  // The final node count could be smaller than the initial count if there was a
  // parser error.
  assert(node_count <= initial_node_count);

  // The log is valid. Commit the nodes into the state graph. First make sure
  // that the hash table has at least one bucket for each node in this deps log.
  state->paths_.reserve(node_count);
  nodes_.resize(node_count);
  ParallelMap(thread_pool.get(), IntegralRange<int>(0, node_count),
      [this, state, &log, &node_index](int node_id) {
    size_t index = node_index[node_id];
    size_t header = log.table[index];
    size_t size = RecordSizeInWords(header);
    const char* path = reinterpret_cast<const char*>(&log.table[index + 1]);
    size_t path_size = (size - 2) * sizeof(uint32_t);
    if (path[path_size - 1] == '\0') --path_size;
    if (path[path_size - 1] == '\0') --path_size;
    if (path[path_size - 1] == '\0') --path_size;
    // It is not necessary to pass in a correct slash_bits here. It will
    // either be a Node that's in the manifest (in which case it will
    // already have a correct slash_bits that GetNode will look up), or it
    // is an implicit dependency from a .d which does not affect the build
    // command (and so need not have its slashes maintained).
    Node* node = state->GetNode(StringPiece(path, path_size), 0);
    assert(node->id() < 0);
    node->set_id(node_id);
    nodes_[node_id] = node;
  });

  // Add the deps records.
  deps_.resize(node_count);
  std::vector<size_t> unique_counts = ParallelMap(thread_pool.get(),
      SplitByThreads(node_count),
      [this, &log, &dep_index](std::pair<int, int> node_chunk) {
    size_t unique_count = 0;
    for (int node_id = node_chunk.first; node_id < node_chunk.second; ++node_id) {
      ssize_t index = dep_index[node_id];
      if (index == -1) continue;
      ++unique_count;
      size_t header = log.table[index];
      size_t size = RecordSizeInWords(header);
      assert(size != 0 && IsDepsRecordHeader(header));
      int output_id = log.table[index + 1];
      TimeStamp mtime;
      mtime = (TimeStamp)(((uint64_t)(unsigned int)log.table[index+3] << 32) |
                          (uint64_t)(unsigned int)log.table[index+2]);
      int deps_count = size - 4;
      Deps* deps = new Deps(mtime, deps_count);
      for (int i = 0; i < deps_count; ++i) {
        int input_id = log.table[index + 4 + i];
        Node* node = nodes_[input_id];
        assert(node != nullptr);
        deps->nodes[i] = node;
      }
      deps_[output_id] = deps;
    }
    return unique_count;
  });
  size_t unique_dep_record_count = std::accumulate(unique_counts.begin(),
                                                   unique_counts.end(), 0);

  const size_t actual_file_size = log.file->content().size();
  const size_t parsed_file_size = kFileHeaderSize +
      (chunks.empty() ? 0 : chunks.back().stop) * sizeof(uint32_t);
  assert(parsed_file_size <= actual_file_size);
  if (parsed_file_size < actual_file_size) {
    // An error occurred while loading; try to recover by truncating the file to
    // the last fully-read record.
    *err = "premature end of file";
    log.file.reset();

    if (!Truncate(path, parsed_file_size, err))
      return false;

    // The truncate succeeded; we'll just report the load error as a
    // warning because the build can proceed.
    *err += "; recovering";
    return true;
  }

  // Rebuild the log if there are too many dead records.
  const unsigned kMinCompactionEntryCount = 1000;
  const unsigned kCompactionRatio = 3;
  if (total_dep_record_count > kMinCompactionEntryCount &&
      total_dep_record_count > unique_dep_record_count * kCompactionRatio) {
    needs_recompaction_ = true;
  }

  return true;
}

DepsLog::Deps* DepsLog::GetDeps(Node* node) {
  // Abort if the node has no id (never referenced in the deps) or if
  // there's no deps recorded for the node.
  if (node->id() < 0 || node->id() >= (int)deps_.size())
    return NULL;
  return deps_[node->id()];
}

bool DepsLog::Recompact(const string& path, const DiskInterface& disk, string* err) {
  METRIC_RECORD(".ninja_deps recompact");

  Close();
  string temp_path = path + ".recompact";

  // OpenForWrite() opens for append.  Make sure it's not appending to a
  // left-over file from a previous recompaction attempt that crashed somehow.
  unlink(temp_path.c_str());

  DepsLog new_log;
  if (!new_log.OpenForWrite(temp_path, disk, err))
    return false;

  // Clear all known ids so that new ones can be reassigned.  The new indices
  // will refer to the ordering in new_log, not in the current log.
  for (vector<Node*>::iterator i = nodes_.begin(); i != nodes_.end(); ++i)
    (*i)->set_id(-1);
  
  // Write out all deps again.
  for (int old_id = 0; old_id < (int)deps_.size(); ++old_id) {
    Deps* deps = deps_[old_id];
    if (!deps) continue;  // If nodes_[old_id] is a leaf, it has no deps.

    Node* node = nodes_[old_id];
    if (node->in_edge()) {
      // If the current manifest defines this edge, skip if it's not dep
      // producing.
      if (node->in_edge()->GetBinding("deps").empty()) continue;
    } else {
      // If the current manifest does not define this edge, skip if it's missing
      // from the disk.
      string err;
      TimeStamp mtime = disk.Stat(node->path(), &err);
      if (mtime == -1)
        Error("%s", err.c_str()); // log and ignore Stat() errors
      if (mtime == 0)
        continue;
    }

    if (!new_log.RecordDeps(nodes_[old_id], deps->mtime,
                            deps->node_count, deps->nodes)) {
      new_log.Close();
      return false;
    }
  }

  new_log.Close();

  // All nodes now have ids that refer to new_log, so steal its data.
  deps_.swap(new_log.deps_);
  nodes_.swap(new_log.nodes_);

  if (unlink(path.c_str()) < 0) {
    *err = strerror(errno);
    return false;
  }

  if (rename(temp_path.c_str(), path.c_str()) < 0) {
    *err = strerror(errno);
    return false;
  }

  return true;
}

bool DepsLog::IsDepsEntryLiveFor(Node* node) {
  // Skip entries that don't have in-edges or whose edges don't have a
  // "deps" attribute. They were in the deps log from previous builds, but the
  // files they were for were removed from the build.
  return node->in_edge() && !node->in_edge()->GetBinding("deps").empty();
}

bool DepsLog::UpdateDeps(int out_id, Deps* deps) {
  if (out_id >= (int)deps_.size())
    deps_.resize(out_id + 1);

  bool delete_old = deps_[out_id] != NULL;
  if (delete_old)
    delete deps_[out_id];
  deps_[out_id] = deps;
  return delete_old;
}

bool DepsLog::RecordId(Node* node) {
  int path_size = node->path().size();
  int padding = (4 - path_size % 4) % 4;  // Pad path to 4 byte boundary.

  unsigned size = path_size + padding + 4;
  if (size > kMaxRecordSize) {
    errno = ERANGE;
    return false;
  }
  if (fwrite(&size, 4, 1, file_) < 1)
    return false;
  if (fwrite(node->path().data(), path_size, 1, file_) < 1) {
    assert(node->path().size() > 0);
    return false;
  }
  if (padding && fwrite("\0\0", padding, 1, file_) < 1)
    return false;
  int id = nodes_.size();
  unsigned checksum = ~(unsigned)id;
  if (fwrite(&checksum, 4, 1, file_) < 1)
    return false;
  if (fflush(file_) != 0)
    return false;

  node->set_id(id);
  nodes_.push_back(node);

  return true;
}
