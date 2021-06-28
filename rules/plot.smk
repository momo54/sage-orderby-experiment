from scripts.utils import list_files, query_name
from glob import glob
from re import search

def allprepared(wildcards):
    print(os.getcwd())
    f=glob("workloads/watdiv/*.rq")
    res=[]
    #print(f'f:{f}')
    for e in f:
        m = search('workloads/watdiv/(.*).rq', e)
        res.append(f'output/orderby/{m.group(1)}-prepared.csv')
        res.append(f'output/baseline/{m.group(1)}-prepared.csv')
    print(f'res:{res}')
    return res


rule prepare_data:
    input:
        "output/{engine}/{query}.csv"
    output:
        prepared="output/{engine}/{query}-prepared.csv",
    run:
        with open(output.prepared, "w") as out:
            for l in open(input[0]):
                print(f"{wildcards.query},{wildcards.engine},{l}", file=out)

rule merge_engine_data:
     input: allprepared
     output:
         "output/all.csv"
     run:
         with open(output[0], "w") as out:
             print(f"input:{input}")
             print("query,engine,execution_time,nb_calls,nb_results,loading_time,resume_time",file=out)
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
