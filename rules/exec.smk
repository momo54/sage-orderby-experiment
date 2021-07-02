def cli(wildcards):
    res=""
    if wildcards.engine=="baseline":
        res+="--no-orderby"
    else:
        res+="--orderby"
    res+=f" --limit {wildcards.limit}"
    print(f'cli:{res}')
    return res


rule run_sage_baseline:
    input:
        ancient("workloads/watdiv/{query}.rq")
    output:
        result="output/baseline/{limit}/{query,[^/]+}.json",
        stats="output/baseline/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew"
    shell:
        "python scripts/query_sage.py {input} \
                {params.endpoint}  {params.dataset}\
                --no-orderby --limit {wildcards.limit}\
                --output {output.result} --measures {output.stats}"


rule run_sage_orderby_server:
    input:
        ancient("workloads/watdiv/{query}.rq")
    output:
        result="output/orderby/{limit}/{query,[^/]+}.json",
        stats="output/orderby/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew"
    shell:
        "python scripts/query_sage.py {input} \
                {params.endpoint}  {params.dataset}\
                --orderby --limit {wildcards.limit}\
                --output {output.result} --measures {output.stats}"

rule run_sage_orderbyone:
    input:
        ancient("workloads/watdiv/{query}.rq")
    output:
        result="output/orderbyone/{limit}/{query,[^/]+}.json",
        stats="output/orderbyone/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew",
        sagepath="/Users/molli-p/orderby/sage-engine"
    shell:
        "PYTHONPATH={params.sagepath} python scripts/query_orderby.py  \
                {params.endpoint}  {params.dataset}\
                --limit {wildcards.limit}\
                -f {input} --output {output.result} --measures {output.stats}"
