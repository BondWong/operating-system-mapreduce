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
	std::vector<masterworker::Result> results;
	std::vector<std::thread> threads;
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

		masterworker::Result res;
		threads.push_back(workerPool.executeMap(&shard, &res));
		results.push_back(res);
	}

	// block till all map jobs finished
	for (auto& t: threads) t.join();

	// do reduce works with blocking queue, thread pool idea from my last assignment
	int region_id = 1;
	int region_size = results.size() / mr_spec.n_output_files;
	std::vector<masterworker::Result> reduceResults;
	std::vector<std::thread> reduceThreads;
	int i = 0;
	while (i < results.size()) {
		int j = i;
		masterworker::Region region;
		region.set_id(region_id++);
		while (j < results.size() && j < region_size) {
			region.add_file_paths(results.at(i).res.file_path);
			j++;
		}

		masterworker::Result res;
		reduceThreads.push_back(workerPool.executeReduce(&region, &res));
		reduceResults.push_back(res);
		i = j;
	}

	// block till all reduce jobs finished
	for (auto& t: reduceThreads) t.join();
	return true;
}
