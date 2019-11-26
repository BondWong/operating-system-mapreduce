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

	std::unique_ptr<masterworker::WorkerService::Stub>& get_worker_stub();
	void release_worker(const std::string& worker_ipaddr_port);
	bool done();

private:
	int size;
	std::map<std::string, std::unique_ptr<masterworker::WorkerService::Stub> > workers;
	std::queue<std::string> free_worker_queue;
	std::mutex mutex;
	std::condition_variable condition;

	std::string get_worker();
};

WorkerPool::WorkerPool(const std::vector<std::string>& worker_ipaddr_ports) {
	for (auto& worker_ipaddr_port: worker_ipaddr_ports) {
		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(worker_ipaddr_port,
			grpc::InsecureChannelCredentials());
		std::unique_ptr<masterworker::WorkerService::Stub> stub(masterworker::WorkerService::NewStub(channel));
		workers.insert(std::make_pair(worker_ipaddr_port, std::move(stub)));
		free_worker_queue.push(worker_ipaddr_port);
	}
	size = free_worker_queue.size();
}

std::unique_ptr<masterworker::WorkerService::Stub>& WorkerPool::get_worker_stub() {
	std::string worker = get_worker();
	std::unique_ptr<masterworker::WorkerService::Stub>& stub_ = workers.at(worker);
	return stub_;
}

void WorkerPool::release_worker(const std::string& worker_ipaddr_port) {
	std::unique_lock<std::mutex> lock(mutex);
	free_worker_queue.push(worker_ipaddr_port);
	lock.unlock();
}

bool WorkerPool::done() {
	return size == free_worker_queue.size();
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
