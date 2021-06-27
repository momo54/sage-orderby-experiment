rule run_sage_ob:
    input:
        ancient("workloads/orderby/{query}.rq")
    output:
        result="output/orderby/{query,[^/]+}.json",
        stats="output/orderby/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
    shell:
        "python scripts/query_sage.py {input} \
                http://localhost:8080/sparql http://example.org/watdiv-skew \
                --output {output.result} --measures {output.stats}"

rule run_sage_baseline:
    input:
        ancient("workloads/orderby/{query}.rq")
    output:
        result="output/baseline/{query,[^/]+}.json",
        stats="output/baseline/{query,[^/]+}.csv",
    params:
        endpoint="http://localhost:8080/sparql",
    shell:
        "python scripts/query_sage.py --orderby {input} \
                http://localhost:8080/sparql http://example.org/watdiv-skew \
                --output {output.result} --measures {output.stats}"
