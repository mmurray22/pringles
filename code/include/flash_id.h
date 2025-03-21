// FlashID struct

#include <string>
#include <cstdint>

/*
 * FlashID
 *
 * This struct holds flash unit identification and IP metadata.
 */

class FlashID {
    public:
        uint64_t id;
        std::string ip;
}