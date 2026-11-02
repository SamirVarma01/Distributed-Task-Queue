# Discarded runs

Results kept for the record but left out of `summary.md` and of the medians in the README, which read only `bench/results/*.json`.

## matrix-20261102-124316-*-rep2

The second repetition of the `sleep_ms` matrix. The laptop's lid was closed 32 s into its first run (`w1-c1-rep2`), and the machine slept from 12:50:46 to 13:03:19 EST (`pmset -g log`: "Clamshell Sleep", then a wake from "lid"). That run took 813 s instead of about 160. The four runs after the wake were also slower than repetitions 1 and 3. The whole repetition was set aside because the machine slept during it, and a fourth repetition of all five configurations (`-rep4`) was run in its place, in the same order.
