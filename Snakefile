import yaml
import glob
import pandas


def list_files(path):
    if not os.path.isdir(path):
        return glob.glob(path)
    files = list()
    for filename in os.listdir(path):
        if filename.endswith(".sparql"):
            files.append(f"{path}/{filename}")
    return files


def load_queries(path):
    queries = list()
    for file in list_files(path):
        with open(file, 'r') as reader:
            filename = os.path.basename(file).split('.')[0]
            query = reader.read()
            queries.append((filename, query))
    return queries


def run_files(wcs):
    files = []
    output = "output" if "output" not in config else config["output"]
    for workload in config["xp"]["workloads"]:
        for approach in config["xp"]["approaches"]:
            for filename, query in load_queries(f"workloads/{workload}"):
                for limit in config["xp"]["limits"]:
                    for quota in config["xp"]["quotas"]:
                        for run in config["xp"]["runs"]:
                            files.append((
                                f"{output}/tmp/{workload}/{approach}"
                                f"/{limit}-{quota}/{run}/{filename}.csv"))
    return files


def check_files(wcs):
    files = []
    output = "output" if "output" not in config else config["output"]
    for workload in config["xp"]["workloads"]:
        for approach in config["xp"]["approaches"]:
            for filename, query in load_queries(f"workloads/{workload}"):
                for limit in config["xp"]["limits"]:
                    for quota in config["xp"]["quotas"]:
                        files.append((
                            f"{output}/tmp/{workload}/{approach}"
                            f"/{limit}-{quota}/check/{filename}.csv"))
    return files


def xp_archive(wcs):
    output = "output" if "output" not in config else config["output"]
    return ancient(f"{output}/xp.tar.gz")


onsuccess: shell("bash scripts/server.sh stop all")
onerror: shell("bash scripts/server.sh stop all")
onstart: shell("bash scripts/server.sh start all")


rule run_all:
    input: xp_archive


rule create_archive:
    input: ancient(expand("{{output}}/{file}.csv", file=["check", "run"]))
    output: "{output}/xp.tar.gz"
    shell: "tar -zcvf {output} --transform 's/{wildcards.output}\\/tmp\\///' {input}"


rule merge_run_topk_query:
    input: ancient(run_files)
    output: "{output}/run.csv"
    shell: "awk 'FNR==1 && NR!=1{{next;}}{{print}}' {input} | sed '/^\\s*$/d' > {output}"


rule format_run_topk_query:
    input:
        ancient(
            "{output}/data/{workload}/{approach}/{limit}-{quota}/{run}/{query}.csv")
    output:
        "{output}/tmp/{workload}/{approach}/{limit}-{quota}/{run,[0-9]}/{query}.csv"
    run:
        df = pandas.read_csv(str(input))
        if "query" not in df:
            df["query"] = wildcards.query
        if "run" not in df:
            df["run"] = wildcards.run
        if "limit" not in df:
            df["limit"] = wildcards.limit
        if "quota" not in df:
            df["quota"] = wildcards.quota
        if "approach" not in df:
            df["approach"] = wildcards.approach
        if "workload" not in df:
            df["workload"] = wildcards.workload
        df.to_csv(str(output), index=False)


rule run_topk_query:
    input:
        query = ancient("workloads/{workload}/{query}.sparql"),
        config = ancient("config/xp.yaml")
    output:
        metrics = "{output}/data/{workload}/{approach}/{limit}-{quota}/{run,[0-9]}/{query}.csv",
        solutions = "{output}/data/{workload}/{approach}/{limit}-{quota}/{run,[0-9]}/{query}.json"
    shell:
        "python scripts/cli.py topk-run \
            {input.config} {input.query} {wildcards.approach} \
            --stats {output.metrics} \
            --output {output.solutions} \
            --limit {wildcards.limit} \
            --quota {wildcards.quota}"


rule merge_check_topk_query:
    input: ancient(check_files)
    output: "{output}/check.csv"
    shell: "awk 'FNR==1 && NR!=1{{next;}}{{print}}' {input} | sed '/^\\s*$/d' > {output}"


rule format_check_topk_query:
    input:
        ancient(
            "{output}/data/{workload}/{approach}/{limit}-{quota}/check/{query}.csv")
    output:
        "{output}/tmp/{workload}/{approach}/{limit}-{quota}/check/{query}.csv"
    run:
        df = pandas.read_csv(str(input))
        if "query" not in df:
            df["query"] = wildcards.query
        if "limit" not in df:
            df["limit"] = wildcards.limit
        if "approach" not in df:
            df["approach"] = wildcards.approach
        if "workload" not in df:
            df["workload"] = wildcards.workload
        df.to_csv(str(output), index=False)


rule check_topk_query:
    input:
        reference = ancient(
            "{output}/data/{workload}/virtuoso/{limit}-0/1/{query}.json"),
        actual = ancient(
            "{output}/data/{workload}/{approach}/{limit}-{quota}/1/{query}.json")
    output:
        "{output}/data/{workload}/{approach}/{limit}-{quota}/check/{query}.csv"
    shell:
        "python scripts/cli.py compare {input.reference} {input.actual} \
            --output {output}"
