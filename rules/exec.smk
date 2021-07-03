from scripts.utils import list_files, query_name
from glob import glob
from re import search

def todo(wildcards):
    print(os.getcwd())
    res=[]
    for workload in ["watdiv","watdiv-desc"]:
        f=glob(f"workloads/{workload}/*.rq")
        for e in f:
            m = search(f'workloads/{workload}/(.*).rq', e)
            for engine in ["orderby","baseline","orderbyone"]:
                for limit in [1,10,20]:
                    res.append(f'output/{workload}/{engine}/{limit}/{m.group(1)}.csv')
        print(f'todo:{res}')
    return res

def cli(wildcards):
    res=""
    if wildcards.engine=="baseline":
        res+="--no-orderby"
    else:
        res+="--orderby"
    res+=f" --limit {wildcards.limit}"
    print(f'cli:{res}')
    return res

rule compute_all:
     input: todo
     output:
         "output/all.csv"
     run:
         with open(output[0], "w") as out:
             print(f"input:{input}")
             print("query,engine,limit,execution_time,nb_calls,nb_results,loading_time,resume_time,workload",file=out)
             for f in input:
                 for l in open(f):
                     print(f"{l}", file=out)


rule run_sage_baseline:
    input:
        ancient("workloads/{workload}/{query}.rq")
    output:
        result="output/{workload}/baseline/{limit}/{query,[^/]+}.json",
        stats="output/{workload}/baseline/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew"
    shell:
        "python scripts/query_sage.py {input} \
                {params.endpoint}  {params.dataset}\
                --no-orderby --limit {wildcards.limit}\
                --output {output.result} --measures {output.stats} --tags {wildcards.workload}"


rule run_sage_orderby_server:
    input:
        ancient("workloads/{workload}/{query}.rq")
    output:
        result="output/{workload}/orderby/{limit}/{query,[^/]+}.json",
        stats="output/{workload}/orderby/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew"
    shell:
        "python scripts/query_sage.py {input} \
                {params.endpoint}  {params.dataset}\
                --orderby --limit {wildcards.limit}\
                --output {output.result} --measures {output.stats} --tags {wildcards.workload}"

rule run_sage_orderbyone:
    input:
        ancient("workloads/{workload}/{query}.rq")
    output:
        result="output/{workload}/orderbyone/{limit}/{query,[^/]+}.json",
        stats="output/{workload}/orderbyone/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew",
        sagepath="/Users/molli-p/orderby/sage-engine"
    shell:
        "PYTHONPATH={params.sagepath} python scripts/query_orderby.py  \
                {params.endpoint}  {params.dataset}\
                --limit {wildcards.limit}\
                -f {input} --output {output.result} --measures {output.stats} --tags {wildcards.workload}"
