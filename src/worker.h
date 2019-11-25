#pragma once

#include <string>
#include <fstream>
#include <sstream>
#include <grpcpp/grpcpp.h>
#include <grpc/support/log.h>

#include <mr_task_factory.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */

// the idea of creating a service is based on this link https://grpc.io/docs/tutorials/basic/cpp/
class Worker final: public masterworker::MapReduceWorkerService::Service {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		int map_number;
		std::string ip_addr_port;

		grpc::Status map(grpc::ServerContext* ctx, const masterworker::Shard* shard, masterworker::Result* res) override {
			auto mapper = get_mapper_from_task_factory("cs6210");

			for (int i = 0; i < shard->components_size(); i++) {
				const masterworker::ShardComponent& comp = shard->components(i);
				const std::string& file_path = comp.file_path();
				int start = comp.start();
				int size = comp.size();

				std::ifstream source_file(file_path);
				if (!source_file.is_open()) {
					std::cerr << "Error when opening file: " << file_path << std::endl;
					return new grpc::Status(grpc::Status::INTERNAL,
						"Error when opening file: " + file_path);
				}

				std::string line;
				// loop to the starting line
				for (int j = 0; j < start; j++) std::getline(source_file, line);
				for (int j = start; j < size; j++) {
					std::getline(source_file, line);
					mapper->map(line);
				}
			}

			std::vector<std::pair<std::string, std::string> >& key_vals = mapper->impl_->pairs;
			sort(key_vals.begin(), key_vals.end());

			std::string output_filepath("Worker_" + ip_addr_port + "_" + std::to_string(++map_number));
			std::ofstream output_file(output_filepath);
			if (!output_file.is_open()) {
				std::cerr << "Error when opening an output file for map function: " << output_filepath << std::endl;
				return new grpc::Status(grpc::Status::INTERNAL,
					"Error when opening an output file for map function: " + output_filepath);
			}

			std::vector<std::pair<std::string, std::string> >::iterator it;
			for(it = key_vals.begin(); it != key_vals.end(); it++) output_file << it->first << " " << it->second << std::endl;

			res->set_worker_ip_addr_port(ip_addr_port);
			res->set_file_path(output_filepath);
			return grpc::Status::OK;
		}

		grpc::Status reduce(grpc::ServerContext* ctx, const masterworker::Region* region, masterworker::Result* res) override {
			auto reducer = get_reducer_from_task_factory("cs6210");

			for (int i = 0; i < region->file_paths_size(); i++) {
				std::ifstream source_file(region->file_paths(i));
				std::string line;
				std::vector<std::string> vals;
				std::string prev_key;

				while (std::getline(source_file, line)) {
					std::istringstream iss(line);
					std::string key, val;
					if (!std::getline(iss, key, ' ') || !std::getline(iss, val)) {
						std::cerr << "Error when processing intermediate file in reduce function: " << region->file_paths(i) << std::endl;
						return new grpc::Status(grpc::Status::INTERNAL,
							"Error when processing intermediate file in reduce function: " + region->file_paths(i));
					}

					if (prev_key.compare("") != 0 && prev_key.compare(key) != 0) {
						reducer->reduce(prev_key, vals);
						vals.clear();
					}

					vals.push_back(val);
					prev_key = key;
				}
			}

			// done with region, write to file
			std::string output_filepath("./output/output_" + ip_addr_port);
			std::ofstream output_file(output_filepath);
			if (!output_file.is_open()) {
				std::cerr << "Error when opening an output file for reduce function: " << output_filepath << std::endl;
				return new grpc::Status(grpc::Status::INTERNAL,
					"Error when opening an output file for reduce function: " + output_filepath);
			}

			std::vector<std::pair<std::string, std::string> >& key_vals = reducer->impl_->pairs;
			std::vector<std::pair<std::string, std::string> >::iterator it;
			for(it = key_vals.begin(); it != key_vals.end(); it++) output_file << it->first << "=" << it->second << std::endl;
			output_file.close();

			res->set_worker_ip_addr_port(ip_addr_port);
			res->set_file_path(output_filepath);

			return grpc::Status::OK;
		}
};


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port): ip_addr_port(ip_addr_port) {
	map_number = 0;
}

/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
		Remove them once you start writing your own logic */
	std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	auto mapper = get_mapper_from_task_factory("cs6210");
	mapper->map("I m just a 'dummy', a \"dummy line\"");
	auto reducer = get_reducer_from_task_factory("cs6210");
	reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	return true;
}
