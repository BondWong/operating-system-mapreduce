#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <iostream>

/* CS6210_TASK: Create your data structure here for storing spec from the config file */
struct MapReduceSpec {
	std::string user_id;
	int n_workers;
	std::vector<std::string> worker_ipaddr_ports;
	std::vector<std::string> input_files;
	int map_kilobytes; // shard size
	int n_output_files; // the number of regions R
	std::string output_dir;
};

inline bool fill_marspec(MapReduceSpec& mr_spec, const std::string& key, const std::string& val) {
	if (key.compare("user_id") == 0) {
		mr_spec.user_id = val;
	} else if (key.compare("n_workers") == 0) {
		mr_spec.n_workers = std::stoi(val);
	} else if (key.compare("worker_ipaddr_ports") == 0) {
		std::istringstream iss(val);
		std::string file_path;
		while (std::getline(iss, file_path, ',')) mr_spec.worker_ipaddr_ports.push_back(file_path);
	} else if (key.compare("input_files") == 0) {
		std::istringstream iss(val);
		std::string file_path;
		while (std::getline(iss, file_path, ',')) mr_spec.input_files.push_back(file_path);
	} else if (key.compare("map_kilobytes") == 0) {
		mr_spec.map_kilobytes = std::stoi(val);
	} else if (key.compare("n_output_files") == 0) {
		mr_spec.map_kilobytes = std::stoi(val);
	} else if (key.compare("output_dir") == 0) {
		mr_spec.output_dir = val;
	} else {
		return false;
	}

	return true;
}

/* CS6210_TASK: Populate MapReduceSpec data structure with the specification from the config file */
inline bool read_mr_spec_from_config_file(const std::string& config_filename, MapReduceSpec& mr_spec) {
	std::ifstream config_file(config_filename);
	if (!config_file.is_open()) {
		std::cerr << "Error when opening file: " << config_filename << std::endl;
		return false;
	}

	std::string line;
	while (std::getline(config_file, line)) {
		// split line
		std::istringstream line_stream(line);
		std::string key;
		std::string value;
		bool isOk = std::getline(line_stream, key, '=')
			&& std::getline(line_stream, value);

		if (!isOk) {
			std::cerr << "Error when parsing line: " << line << std::endl;
			return false;
		}

		// fill spec
		if (!fill_marspec(mr_spec, key, value)) {
			std::cerr << "Error when setting: " << key << " " << value << std::endl;
			return false;
		}
	}
	config_file.close();
	return true;
}


/* CS6210_TASK: validate the specification read from the config file */
inline bool validate_mr_spec(const MapReduceSpec& mr_spec) {
	// debug
	std::cout << "user_id" << mr_spec.user_id <<std::endl;
	std::cout << "n_workers" << mr_spec.n_workers <<std::endl;
	for (auto i = mr_spec.worker_ipaddr_ports.begin(); i != mr_spec.worker_ipaddr_ports.end(); i++) std::cout << *i << ' ';
	std::cout << std::endl;
	for (auto i = mr_spec.worker_ipaddr_ports.begin(); i != mr_spec.input_files.end(); i++) std::cout << *i << ' ';
	std::cout << std::endl;
	std::cout << "map_kilobytes" << mr_spec.map_kilobytes <<std::endl;
	std::cout << "n_output_files" << mr_spec.n_output_files <<std::endl;
	std::cout << "output_dir" << mr_spec.output_dir <<std::endl;
	return true;
}
