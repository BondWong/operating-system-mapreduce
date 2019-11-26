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
	std::vector<ShardComponent> comps;
	cur_shard->components = comps;
	int cur_size = 0;
	int start_line = 0;
	int line_cnt = 0;

	std::vector<std::string>::const_iterator it = mr_spec.input_files.begin();
	while (it != mr_spec.input_files.end()) {
		std::ifstream intput_file(*it);
		if (!intput_file.is_open()) {
			std::cerr << "Error when opening file: " << *it << std::endl;
			return false;
		}

		std::string line;
		while (std::getline(intput_file, line)) {
			// find a shard
			if (cur_size + line.length() > shard_size) {
				std::cout << "Found a shard of size: " << cur_size << std::endl;
				ShardComponent* component = new ShardComponent;
				component->file_path = *it;
				component->start = start_line;
				component->size = line_cnt - start_line;
				cur_shard->components.push_back(*component);
				fileShards.push_back(*cur_shard);

				// clear for next shard
				start_line = line_cnt;
				cur_size = 0;
				fileshard_id++;
				cur_shard = new FileShard;
				cur_shard->id = fileshard_id;
				std::vector<ShardComponent> comps;
				cur_shard->components = comps;

				// print_component(component);
			}

			cur_size += line.length();
			line_cnt++;
		}

		// put remaining file to a shard as a component
		if (cur_size > 0) {
			std::cout << "Hanle remaining component of size: " << cur_size << std::endl;
			ShardComponent* component = new ShardComponent;
			component->file_path = *it;
			component->start = start_line;
			component->size = line_cnt - start_line;
			cur_shard->components.push_back(*component);

			// print_component(component);
		}

		// clear for next file
		start_line = 0;
		line_cnt = 0;

		// handle next file
		it++;
	}

	return true;
}
