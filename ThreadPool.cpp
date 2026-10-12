#include "ThreadPool.h"
#include <stdio.h>

ThreadPool::ThreadPool (size_t n) {
	start(n);
};
ThreadPool::~ThreadPool () {
	stop();
};
void ThreadPool::init (size_t n) {
	if (!started) {
		start(n);
		started = true;
	}
}
void ThreadPool::enqueue (function<void(int)> job) {
	{
		unique_lock<mutex> lk(mutex_);
		jobs_.push(move(job));
	}
	cv_.notify_one();
}
#ifdef _DEBUG
void ThreadPool::printThrdStats () {
	char buf[512];
	char* b = buf;
	for (int i=0; i<isRunning_.size(); ++i) {
		b += sprintf(b, "%d: %d, ", i, isRunning_[i]);
	}
	b-=2;
	*b='\0';
}
#else
void ThreadPool::printThrdStats () {}
#endif

void ThreadPool::start (size_t n) {
	// Pre-size before spawning: workers write isRunning_[tid] as soon as
	// they start, which races with push_back growth (out-of-bounds access
	// and reallocation while a worker is inside the vector).
	isRunning_.resize(n, 0);
	for (size_t i=0; i<n; ++i) {
		workers_.emplace_back([this,i] () {
			tid = i;
			while (true) {
				function<void(int)> job; {
					unique_lock<mutex> lk(mutex_);
					if (jobs_.empty() && !jc) {
						cvJoin_.notify_all();
					}
					cv_.wait(lk, [this] {
						printThrdStats();
						return stopping_ || !jobs_.empty();
					});
					if (stopping_ && jobs_.empty()) {
						printf("stopping %d\n", tid);
						printThrdStats();
						return;
					}
					job = move(jobs_.front());
					jobs_.pop();
				}
				try {
					++jc;
					isRunning_[tid]=1;
					job(tid);
					isRunning_[tid]=0;
					printThrdStats();
					--jc;
				} catch (const exception &e) {
					printf("worker exception: %s\n", e.what());
				} catch (...) {
					printf("worker exception: unknown");
				}
			}
		});
	}
}

void ThreadPool::stop () {
	{
		unique_lock<mutex> lk(mutex_);
		stopping_ = true;
	}
	cv_.notify_all();
	for(auto &t : workers_)
		if (t.joinable())
			t.join();
}
void ThreadPool::join () {
	unique_lock<mutex> lk(mutex_);
	cvJoin_.wait(lk, [this] {
		return jobs_.empty();
	});
}
