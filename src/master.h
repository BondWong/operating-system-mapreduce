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
			grpc::Status status;
			masterworker::Result res;
			std::unique_ptr<grpc::ClientAsyncResponseReader<masterworker::Result> > response_reader;
		};

		WorkerPool* workerPool;
		const MapReduceSpec& mr_spec;
		const std::vector<FileShard>& file_shards;
		std::vector<std::string> mapFiles;

		grpc::CompletionQueue cq_map;
		void executeMap(const masterworker::Shard& shard);
		void asyncCompleteRpcMap();
		std::thread mapRepDaemonThread;

		grpc::CompletionQueue cq_reduce;
		void executeReduce(const masterworker::Shard& region);
		void asyncCompleteRpcReduce();
		std::thread reduceRepDaemonThread;
};

/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate ycomponent->set_file_path((*component_it).file_path);our other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards)
 	: mr_spec(mr_spec), file_shards(file_shards) {
	workerPool = new WorkerPool(mr_spec.worker_ipaddr_ports);
	mapRepDaemonThread = std::thread(&Master::asyncCompleteRpcMap, this);
	reduceRepDaemonThread = std::thread(&Master::asyncCompleteRpcReduce, this);
}

// const masterworker::Shard& shard, masterworker::Result* res

void Master::executeMap(const masterworker::Shard& shard) {
	// idea from this link https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_client2.cc#L101
	AsyncClientCall* call = new AsyncClientCall;
	std::unique_ptr<masterworker::WorkerService::Stub>& stub_= workerPool->get_worker_stub();
	call->response_reader = stub_->PrepareAsyncMap(&call->context, shard, &cq_map);
	call->response_reader->StartCall();
	call->response_reader->Finish(&call->res, &call->status, (void*) call);
}

void Master::asyncCompleteRpcMap() {
	void *got_tag;
	bool ok = false;
	// wait for the next available response
	while (cq_map.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		GPR_ASSERT(ok);
		if (!call->status.ok()) {
			std::cout << call->status.error_code() << ": " << call->status.error_message() << std::endl;
			return;
		}
		std::string worker = call->res.worker_ipaddr_port();
		std::string fiel_path = call->res.file_path();
		workerPool->release_worker(worker);
		mapFiles.push_back(fiel_path);
		delete call;
	}
}

void Master::executeReduce(const masterworker::Shard& shard) {
	// idea from this link https://github.com/grpc/grpc/blob/master/examples/cpp/helloworld/greeter_async_client2.cc#L101
	AsyncClientCall* call = new AsyncClientCall;
	std::unique_ptr<masterworker::WorkerService::Stub>& stub_= workerPool->get_worker_stub();
	call->response_reader = stub_->PrepareAsyncMap(&call->context, shard, &cq_reduce);
	call->response_reader->StartCall();
	call->response_reader->Finish(&call->res, &call->status, (void*) call);
}

void Master::asyncCompleteRpcReduce() {
	void *got_tag;
	bool ok = false;
	// wait for the next available response
	while (cq_reduce.Next(&got_tag, &ok)) {
		AsyncClientCall* call = static_cast<AsyncClientCall*>(got_tag);
		GPR_ASSERT(ok);
		if (!call->status.ok()) {
			std::cout << call->status.error_code() << ": " << call->status.error_message() << std::endl;
			return;
		}
		std::string worker = call->res.worker_ipaddr_port();
		workerPool->release_worker(worker);
		delete call;
	}
}

/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	// do map works with blocking queue, thread pool idea from my last assignment
	std::vector<FileShard>::const_iterator it;
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
	while (!workerPool->done()) std::this_thread::sleep_for(std::chrono::seconds(1));
	while (mapFiles.size() != file_shards.size()) {
		std::this_thread::sleep_for(std::chrono::seconds(1));
		std::cout << mapFiles.size() << std::endl;
	}

	int total_line_cnt = 0;
	std::vector<std::string>::const_iterator mapRes_it;
	for (mapRes_it = mapFiles.begin(); mapRes_it != mapFiles.end(); mapRes_it++) {
		const std::string& file_path = *mapRes_it;
		std::cout << file_path << std::endl;
		std::ifstream interm_file(file_path);
		total_line_cnt += std::count(std::istreambuf_iterator<char>(interm_file), std::istreambuf_iterator<char>(), '\n');
	}

	std::cout << "total line: " << total_line_cnt << std::endl;

	// do reduce works with blocking queue, thread pool idea from my last assignment
	int region_id = 0;
	int region_size = total_line_cnt / (mr_spec.n_output_files - 1);
	int cur_size = 0;
	int start_line = 0;
	int line_cnt = 0;
	std::vector<masterworker::Shard> regions;

	mapRes_it = mapFiles.begin();
	while (mapRes_it != mapFiles.end()) {
		const std::string& file_path = *mapRes_it;
		std::ifstream interm_file(file_path);
		if (!interm_file.is_open()) {
			std::cerr << "Error when opening file: " << file_path << std::endl;
			return false;
		}

		std::string line;
		while (std::getline(interm_file, line)) {
			// find a shard
			cur_size++;
			line_cnt++;

			if (cur_size == region_size) {
				std::cout << "Found a region of size: " << cur_size << std::endl;
				std::cout << "1" <<std::endl;
				masterworker::Shard region;
				std::cout << "2" <<std::endl;
				region.set_id(region_id);
				std::cout << "3" <<std::endl;
				masterworker::ShardComponent *component = region.add_components();
				std::cout << "4" <<std::endl;
				component->set_file_path(file_path);
				std::cout << "5" <<std::endl;
				component->set_start(start_line);
				std::cout << "6" <<std::endl;
				component->set_size(cur_size);
				std::cout << "7" <<std::endl;
				// clear for next shard
				start_line = line_cnt;
				cur_size = 0;
				region_id++;
				std::cout << "8" <<std::endl;
				regions.push_back(region);
				std::cout << "9" <<std::endl;
			}
		}

		if (cur_size > 0) {
			std::cout << "Found a region of size: " << cur_size << std::endl;
			masterworker::Shard region;
			if (regions.size() != 0) region = regions.back();
			masterworker::ShardComponent *component = region.add_components();
			component->set_file_path(file_path);
			component->set_start(start_line);
			component->set_size(cur_size);
		}

		// clear for next file
		start_line = 0;
		line_cnt = 0;

		// handle next file
		mapRes_it++;
	}

	for (masterworker::Shard& region: regions) executeReduce(region);

	// block till all reduce jobs finished
	while (!workerPool->done()) std::this_thread::sleep_for(std::chrono::seconds(1));

	std::cout << "Press control-c to quit" << std::endl << std::endl;
  mapRepDaemonThread.join();  //blocks forever
	reduceRepDaemonThread.join();  //blocks forever

	return true;
}
