#pragma once

#include <vector>
#include <fstream>
#include "mapreduce_spec.h"

/* CS6210_TASK: Create your own data structure here, where you can hold information about file splits,
     that your master would use for its own bookkeeping and to convey the tasks to the workers for mapping */

struct ShardComponent {
	std::string file_path;
	std::streampos start;
	int size;
};

struct FileShard {
	int id;
	std::vector<ShardComponent> components;
};

inline void print_component(const ShardComponent* component) {
	std::cout << "file_path=" << component->file_path << std::endl;
	std::cout << "start=" << component->start << std::endl;
	std::cout << "size=" << component->size << std::endl;
}

/* CS6210_TASK: Create fileshards from the list of input files, map_kilobytes etc. using mr_spec you populated  */
inline bool shard_files(const MapReduceSpec& mr_spec, std::vector<FileShard>& fileShards) {
	int shard_size = mr_spec.map_kilobytes * 1024;
	int fileshard_id = 0;
	FileShard* cur_shard = new FileShard;
	cur_shard->id = fileshard_id;
	int cur_size = 0;

	std::vector<std::string>::const_iterator it = mr_spec.input_files.begin();
	while (it != mr_spec.input_files.end()) {
		int start_line = 0;
		int line_cnt = 0;

		std::ifstream infile(*it);
		std::string line;
		while (std::getline(infile, line)) {
			// find one component
			if (cur_size + line.length() > shard_size) {
				ShardComponent* component;
				component->file_path = *it;
				component->start = start_line;
				component->size = line_cnt;
				cur_shard->components.push_back(*component);
				fileShards.push_back(*cur_shard);

				// clear for next component
				start_line = line_cnt;
				line_cnt = 0;
				cur_size = 0;
				fileshard_id++;
				cur_shard = new FileShard;

				print_component(component);
			}

			cur_size += line.length();
			line_cnt++;
		}

		// handle remaining
		ShardComponent* component;
		component->file_path = *it;
		component->start = start_line;
		component->size = line_cnt;
		cur_shard->components.push_back(*component);

		print_component(component);
		// handle next file
		it++;
	}

	return true;
}
