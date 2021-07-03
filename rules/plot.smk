from scripts.utils import list_files, query_name
from glob import glob
from re import search

def todo_plot(wildcards):
    print(os.getcwd())
    res=[]
    for workload in ["watdiv","watdiv-desc"]:
        for limit in [1,10,20]:
            res.append(f"figures/{workload}/{limit}/execution_times.png")
            res.append(f"figures/{workload}/{limit}/data_transfer.png")
            res.append(f"figures/{workload}/{limit}/nb_calls.png")
    for workload in ["watdiv","watdiv-desc"]:
        for engine in ["orderby","orderbyone"]:
            res.append(f"figures/{workload}/{engine}/change_limit.png")
            res.append(f"figures/{workload}/{engine}/change_limit_overhead.png")
    for engine in ["baseline","orderby","orderbyone"]:
        for limit in [1,10,20]:
            res.append(f"figures/{engine}/{limit}/workload_comp.png")
    print(f'todo_plot:{res}')
    return res

rule plot_all:
    input: todo_plot

rule plot_execution_times:
    input:
        ancient("output/all.csv")
    output:
        "figures/{workload}/{limit}/execution_times.png"
    shell:
        "python scripts/plots.py execution-times \
                --limit {wildcards.limit} --workload {wildcards.workload} \
                 {input} {output}"

rule plot_data_transfer:
    input:
        ancient("output/all.csv")
    output:
        "figures/{workload}/{limit}/data_transfer.png"
    shell:
        "python scripts/plots.py data-transfer \
        --limit {wildcards.limit} --workload {wildcards.workload} \
        {input} {output}"

rule plot_nb_calls:
    input:
        ancient("output/all.csv")
    output:
        "figures/{workload}/{limit}/nb_calls.png"
    shell:
        "python scripts/plots.py nb-calls \
        --limit {wildcards.limit} --workload {wildcards.workload} \
        {input} {output}"

rule plot_limit_change:
    input:
        ancient("output/all.csv")
    output:
        "figures/{workload}/{engine}/change_limit.png"
    shell:
        "python scripts/plots.py change-limit \
        --engine {wildcards.engine} --workload {wildcards.workload} \
        {input} {output}"

rule plot_limit_change_overhead:
    input:
        ancient("output/all.csv")
    output:
        "figures/{workload}/{engine}/change_limit_overhead.png"
    shell:
        "python scripts/plots.py change-limit-overhead \
                --engine {wildcards.engine} --workload {wildcards.workload} \
                {input} {output}"

rule plot_engine_limit:
    input:
        ancient("output/all.csv")
    output:
        "figures/{engine}/{limit}/workload_comp.png"
    shell:
        "python scripts/plots.py engine-limit \
                --engine {wildcards.engine} --limit {wildcards.limit} \
                {input} {output}"
