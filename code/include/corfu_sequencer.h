#ifndef CORFU_SEQUENCER_H
#define CORFU_SEQUENCER_H

#include <cstdint>
#include <vector>
#include <string>

class CorfuSequencer {
public:
    CorfuSequencer();
    ~CorfuSequencer();

    // Returns the next available token and increments
    uint64_t requestToken();

    // Returns the highest token issued (currentToken - 1)
    uint64_t getHighestToken() const;

private:
    uint64_t currentToken;
};

#endif // CORFU_SEQUENCER_H
