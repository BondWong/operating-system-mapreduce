#pragma once

#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <vector>
#include <map>
#include <queue>
#include <random>
#include <functional>
#include <chrono>
#include <grpc++/grpc++.h>

#include "masterworker.grpc.pb.h"

class WorkerPool {
public:
	WorkerPool(const std::vector<std::string>& worker_ipaddr_ports);
	void executeMap();
	void executeReduce();

private:
	std::map<std::string, std::unique_ptr<masterworker::MapReduceWorkerService::Stub> > workers;
	std::queue<std::string> free_worker_queue;
	std::mutex mutex;
	std::condition_variable condition;

	std::string get_worker();
	void release_worker(std::string& worker_ipaddr_port);
};

WorkerPool::WorkerPool(const std::vector<std::string>& worker_ipaddr_ports) {
	for (auto& worker_ipaddr_port: worker_ipaddr_ports) {
		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(worker_ipaddr_port,
			grpc::InsecureChannelCredentials());
		std::unique_ptr<masterworker::MapReduceWorkerService::Stub> stub(masterworker::MapReduceWorkerService::NewStub(channel));
		std::pair<std::string, std::unique_ptr<masterworker::MapReduceWorkerService::Stub> > (worker_ipaddr_port, stub);
		workers.insert(p);
		free_worker_queue.push(worker_ipaddr_port);
	}
}

std::thread WorkerPool::executeMap(const masterworker::Shard* shard, masterworker::Result* res) {
	std::function<void()> job = [&]() {
		std::string worker = get_worker();
		std::unique_ptr<WorkerService::Stub>& stub_ = workers.at(workder_ipaddr_port);
		grcp::ClientContext context;
		masterworker::Result res;
		grpc::Status status = stub_->map(&context, &shard, &res);
		if (!status.IsOk()) std::cerr << status.error_message() << std::endl;
		release_worker(worker);
	};

	std::thread t(job);
	return t;
}

std::thread WorkerPool::executeReduce(const masterworker::Region* region, masterworker::Result* res) {
	std::function<void()> job = [&]() {
		std::string worker = get_worker();
		std::unique_ptr<WorkerService::Stub>& stub_ = workers.at(workder_ipaddr_port);
		grcp::ClientContext context;
		masterworker::Result res;
		grpc::Status status = stub_->reduce(&context, &region, &res);
		if (!status.IsOk()) std::cerr << status.error_message() << std::endl;
		release_worker(worker);
	};

	std::thread t(job);
	return t;
}

std::string WorkerPool::get_worker() {
	std::unique_lock<std::mutex> lock(mutex);
	while (free_worker_queue.empty()) condition.wait(lock, [&]{ return (!free_worker_queue.empty()); });
	std::string worker_ipaddr_port = free_worker_queue.front();
	free_worker_queue.pop();
	lock.unlock();
	condition.notify_one();
	return worker_ipaddr_port;
}

void WorkerPool::release_worker(std::string& worker_ipaddr_port) {
	std::unique_lock<std::mutex> lock(mutex);
	free_worker_queue.push(worker_ipaddr_port);
	lock.unlock();
}
