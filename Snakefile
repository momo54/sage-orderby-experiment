rule all:
    input:
        "figures/execution_times.png",
        "figures/data_transfer.png",
        "figures/nb_calls.png",
        "figures/change_limit.png",
        "figures/change_limit_overhead.png",

include: "rules/exec.smk"
include: "rules/plot.smk"
