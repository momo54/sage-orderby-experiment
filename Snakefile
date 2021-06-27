rule all:
    input:
        "figures/execution_times.png",
        "figures/data_transfer.png",
        "figures/nb_calls.png",


include: "rules/exec.smk"
include: "rules/plot.smk"
