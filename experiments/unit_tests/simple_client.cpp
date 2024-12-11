#include <iostream>
#include "../code/client/base_client.h"
#include "../code/client"

// Interface for all clients:
// append: adds an entry
// read: reads an entry
// trim: garbage collect
// tail: gets latest committed entries

int main() {
	std::cout << "Testing!" << std::endl;
    BaseClient simpleCli = createClient(config);
    uint64_t idx = simpleCli.append("entry");
    std::string ret = simpleCli.read(idx);
    // Check if entries are the same

	return 0;
}
