CXX = g++
CXXFLAGS = -Wall -O2

all: server client storage

storage: bench_net_storage.cpp
	$(CXX) $(CXXFLAGS) -o storage bench_net_storage.cpp

server: bench_net_server.cpp
	$(CXX) $(CXXFLAGS) -o server bench_net_server.cpp

client: bench_net_client.cpp
	$(CXX) $(CXXFLAGS) -o client bench_net_client.cpp

clean:
	rm -f server client storage
