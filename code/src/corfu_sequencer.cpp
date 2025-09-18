#include "corfu_sequencer.h"

CorfuSequencer::CorfuSequencer() : currentToken(0) {}

CorfuSequencer::~CorfuSequencer() {}

uint64_t CorfuSequencer::requestToken() {
    return currentToken++;
}

uint64_t CorfuSequencer::getHighestToken() const {
    return currentToken == 0 ? 0 : currentToken - 1;
}
