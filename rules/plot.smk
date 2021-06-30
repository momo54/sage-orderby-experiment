from scripts.utils import list_files, query_name
from glob import glob
from re import search

def todo(wildcards):
    print(os.getcwd())
    f=glob("workloads/watdiv/*.rq")
    res=[]
    #print(f'f:{f}')
    for e in f:
        m = search('workloads/watdiv/(.*).rq', e)
        for engine in ["orderby","baseline"]:
            for limit in [1,10,20]:
                res.append(f'output/{engine}/{limit}/{m.group(1)}.csv')
    print(f'todo:{res}')
    return res

rule merge_all:
     input: todo
     output:
         "output/all.csv"
     run:
         with open(output[0], "w") as out:
             print(f"input:{input}")
             print("query,engine,limit,execution_time,nb_calls,nb_results,loading_time,resume_time",file=out)
             for f in input:
                 for l in open(f):
                     print(f"{l}", file=out)

rule plot_execution_times:
    input:
        ancient("output/all.csv")
    output:
        "figures/execution_times.png"
    shell:
        "python scripts/plots.py execution-times {input} {output}"

rule plot_data_transfer:
    input:
        ancient("output/all.csv")
    output:
        "figures/data_transfer.png"
    shell:
        "python scripts/plots.py data-transfer {input} {output}"

rule plot_nb_calls:
    input:
        ancient("output/all.csv")
    output:
        "figures/nb_calls.png"
    shell:
        "python scripts/plots.py nb-calls {input} {output}"


rule plot_limit_change:
    input:
        ancient("output/all.csv")
    output:
        "figures/change_limit.png"
    shell:
        "python scripts/plots.py change-limit {input} {output}"
