# How to Run Experiments
1. Write up a config.toml file (name it that) with the relevant experiment parameters. See sample.toml to understand what the different variables mean.

2. From experiment/scripts, run `python3 experiments.py`

3. Look at the experiment script output to see where to find results and graphs.


# Where to write tests
All tests should be written in the experiment/unit_tests folder, and also added to the meson.build file in that folder (see the other examples in the meson.build file).

## Pringles API for tests in unit_tests
The API that Pringles exposes to be used in the unit_tests is:

```
// Append entries to the log
uint32_t append(std::string entry);
// Read from idx in the log
std::string read(uint64_t idx);
// Get latest committed entry
uint64_t getTail();
// Subscribe to get all log updates after supplied index
void subscribe(uint64_t idx);
// Garbage collect all log entries up to some index
bool trim(uint64_t idx);
// Waits for warmup period
void wait_to_warmup();
// Wait for cooldown period
void wait_to_cooldown();
// This is when experiment stats are actually collected. 
// Boolean indicates True - append testing is happening, False - read testing is happening.
void wait_to_finish(bool is_append);
// Indicates whether the experiment is running or not
bool experiment_status();
```
