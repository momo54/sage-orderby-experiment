def cli(wildcards):
    res=""
    if wildcards.engine=="baseline":
        res+="--no-orderby"
    else:
        res+="--orderby"
    res+=f" --limit {wildcards.limit}"
    print(f'cli:{res}')
    return res


rule run_sage_ob:
    input:
        ancient("workloads/watdiv/{query}.rq")
    output:
        result="output/{engine}/{limit}/{query,[^/]+}.json",
        stats="output/{engine}/{limit}/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
        dataset="http://example.org/watdiv-skew",
        cmd=cli

    shell:
        "python scripts/query_sage.py {input} \
                {params.endpoint}  {params.dataset}\
                {params.cmd} \
                --output {output.result} --measures {output.stats}"
