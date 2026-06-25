CXX = g++
CXXFLAGS = -Wall -O2 -std=c++20

all: bench_switch bench_client bench_storage

bench_storage: bench_net_multithread_basic_server.cpp
	$(CXX) $(CXXFLAGS) -o bench_storage bench_net_multithread_basic_server.cpp

bench_switch: bench_net_server.cpp
	$(CXX) $(CXXFLAGS) -o bench_switch bench_net_server.cpp

bench_client: bench_net_client.cpp
	$(CXX) $(CXXFLAGS) -o bench_client bench_net_client.cpp

clean:
	rm -f bench_switch bench_client bench_storage
