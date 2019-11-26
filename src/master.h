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
		struct AsyncClientCall {
			grpc::ClientContext context;
			Status status;
			masterworker::Result res;
			std::unique_ptr<ClientAsyncResponseReader<InterimFile> > response_reader;
		}

		WorkerPool* workerPool;
		grpc::CompletionQueue cq;
		const MapReduceSpec& mr_spec;
		const std::vector<FileShard>& file_shards;
		std::vector<masterworker::Result> mapResults;

		void executeMap(const masterworker::Shard& shard);
		void asyncCompleteRpcMap();
		std::thread mapRepDaemonThread;

		// std::thread executeReduce(const masterworker::Region& region);
		// void asyncCompleteRpcReduce();
		// std::thread reduceRepDaemonThread;
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate ycomponent->set_file_path((*component_it).file_path);our other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
 	: mr_spec(mr_spec), file_shards(file_shards) {
	workerPool = new WorkerPool(mr_spec.worker_ipaddr_ports);
	mapRepDaemonThread = std::thread(&Master::asyncCompleteRpcMap, this);
}

// const masterworker::Shard& shard, masterworker::Result* res

void Master::executeMap(const masterworker::Shard& shard) {
	// idea from this link https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_client2.cc#L101
	AsyncClientCall* call = new AsyncClientCall;
	std::unique_ptr<masterworker::WorkerService::Stub>& stub_= workerPool->get_worker_stub();
	call->response_reader = stub_->AsyncMap(&call->context, shard, &cq);
	call->response_reader->Finish(&call->reply, &call->status, (void*) call);
}

void Master::asyncCompleteRpcMap() {
	void *got_tag;
	bool ok = false;
	// wait for the next available response
	while (cq.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		GPR_ASSERT(ok);
		if (!call->status.ok()) {
			std::cout << call->status.error_code() << ": " << call->status.error_message() << std::endl;
			return;
		}
		workerPool.release_worker(call->reply.worker_ipaddr_port());
		mapResults.push_back(*(call->reply));
		delete call;
	}
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
			masterworker::ShardComponent *component = shard.add_components();
			component->set_file_path(component_it->file_path);
			component->set_start(component_it->start);
			component->set_size(component_it->size);
		}
		executeMap(shard);
	}

	// block till all map jobs finished
	while (!workerPool->done()) std::this_thread::sleep_for(std::chrono::seconds(5));

	// // do reduce works with blocking queue, thread pool idea from my last assignment
	// int region_id = 1;
	// int region_size = results.size() / mr_spec.n_output_files == 0
	// 	? results.size()
	// 	: results.size() / mr_spec.n_output_files;
	// std::vector<std::thread> reduceThreads;
	// int i = 0;
	// while (i < results.size()) {
	// 	int j = i;
	// 	masterworker::Region region;
	// 	region.set_id(region_id++);
	// 	while (j < results.size() && j < region_size) {
	// 		region.add_file_paths(results.at(i).file_path());
	// 		j++;
	// 	}
	//
	// 	masterworker::Result res;
	// 	reduceThreads.push_back(workerPool->executeReduce(region, &res));
	// 	i = j;
	// }
	//
	// // block till all reduce jobs finished
	// for (auto& t: reduceThreads) t.join();

	return true;
}
