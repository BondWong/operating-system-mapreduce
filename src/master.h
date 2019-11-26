#pragma once

#include <string>
#include <vector>
#include <thread>
#include <grpcpp/grpcpp.h>

#include "mapreduce_spec.h"
#include "file_shard.h"
#include "WorkerPool.h"
#include "masterworker.grpc.pb.h"

/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();

	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		struct JobInfo {
			std::thread t;
			masterworker::Result res;
		};

		WorkerPool workerPool;
		const MapReduceSpec& mr_spec;
		const std::vector<FileShard>& file_shards;

		std::string get_worker();
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate ycomponent->set_file_path((*component_it).file_path);our other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
 	: mr_spec(mr_spec), file_shards(file_shards) {
	workerPool = new WorkerPool(mr_spec.worker_ipaddr_ports);
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// do map works with blocking queue, thread pool idea from my last assignment
	std::vector<FileShard>::const_iterator it;
	std::vector<struct JobInfo> jobInfos;
	for (it = file_shards.begin(); it != file_shards.end(); it++) {
		masterworker::Shard shard;
		shard.set_id(it->id);
		std::vector<ShardComponent>::const_iterator component_it;
		for (component_it = it->components.begin(); component_it != it->components.end(); component_it++) {
			ShardComponent *component = shard.add_components();
			component->set_file_path((*component_it).file_path);
			component->set_start((*component_it).start);
			component->set_size((*component_it).size);
		}

		struct JobInfo jobInfo;
		std::thread t = workerPool.executeMap(&shard, &jobInfo.res);
		jobInfo.t = t;
		jobInfos.push_back(jobInfo);
	}

	// block till all map jobs finished
	for (struct JobInfo &jobInfo: jobInfos) jobInfo.t.join();

	// do reduce works with blocking queue, thread pool idea from my last assignment
	int region_id = 1;
	int region_size = jobInfos / mr_spec.n_output_files;
	std::vector<struct JobInfo> reduceInfos;
	int i = 0;
	while (i < jobInfos.size()) {
		int j = i;
		masterworker::Region region;
		region.set_id(region_id++);
		while (j < jobInfos.size() && j < region_size) {
			region.add_file_paths(jobInfos.at(i).res.file_path);
			j++;
		}

		struct JobInfo reduceJobInfo;
		std::thread t = workerPool.executeReduce(&region, &reduceJobInfo.res);
		reduceJobInfo.t = t;
		reduceInfos.push_back(reduceJobInfo);
		i = j;
	}

	// block till all reduce jobs finished
	for (struct JobInfo &jobInfo: reduceInfos) jobInfo.t.join();
	return true;
}
