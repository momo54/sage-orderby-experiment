#!/usr/bin/python3

import logging
import coloredlogs
import click
import requests
import re

from time import time
from json import dumps
from statistics import mean
from utils import list_files, basename
from pathlib import Path

coloredlogs.install(level='INFO', fmt='%(asctime)s - %(levelname)s %(message)s')
logger = logging.getLogger(__name__)


@click.command()
@click.argument('query', type=click.Path(exists=True, dir_okay=False, file_okay=True))
@click.argument('endpoint', type=str)
@click.argument('default-graph', type=str)
@click.option("--output", type=str, default=None,
    help="The file in which the query result will be stored.")
@click.option("--measures", type=str, default=None,
    help="The file in which query execution statistics will be stored.")
@click.option("--orderby/--no-orderby", default=False,
    help="computes orderby locally.")
@click.option("--limit", type=int, default=10,
    help="Limit of a of SPARQL query.")
def execute(query, endpoint, default_graph, output, measures,orderby,limit):

    orderclause=""
    limitclause=""
    querys=open(query).read()
    query_name=Path(query).stem
    engine="orderby"

    if limit is not None:
        m = re.search('(.*)order by(.*) limit (.*)', querys,re.DOTALL)
        limitclause=int(m.group(3))
        querys=f'{m.group(1)} order by {m.group(2)} limit {limit}'
        print(f"Changing limit from {limit}")

    if orderby is True:
        print(f"removing orderby clause in {querys}")
        # remove order by
        m = re.search('(.*)order by(.*) limit (.*)', querys,re.DOTALL)
        querys=m.group(1)
        print(f'processing:{querys}')
        #keep order expr.
        orderclause=m.group(2)
        limitclause=int(m.group(3))
        engine="baseline"

    headers = {
        "accept": "text/html",
        "content-type": "application/json",
        'Cache-Control': 'no-cache',
        "next": None
    }
    payload = {
        "query": querys,
        "defaultGraph": default_graph,
    }

    has_next = True
    nb_calls = 0
    results = list()
    nb_results = 0
    execution_time = 0
    loading_times = list()
    resume_times = list()

    triples_by_obj = dict()
    max = 0
    obj = ""

    while has_next:
        start_time = time()
        response = requests.post(endpoint, headers=headers, data=dumps(payload))
        execution_time += time() - start_time
        nb_calls += 1

        json_response = response.json()
        has_next = json_response['next']
        payload["next"] = json_response["next"]
        #results.extend(json_response["bindings"])
        nb_results += len(json_response["bindings"])
        loading_times.append(json_response["stats"]["import"])
        resume_times.append(json_response["stats"]["export"])

        # for bindings in json_response["bindings"]:
        #     if bindings["?o"] not in triples_by_obj:
        #         triples_by_obj[bindings["?o"]] = 0
        #     else:
        #         triples_by_obj[bindings["?o"]] += 1
        #     if triples_by_obj[bindings["?o"]] > max:
        #         max = triples_by_obj[bindings["?o"]]
        #         obj = bindings["?o"]

    if orderby is True:
        print(f"need to sort with orderby:{orderclause} limit:{limitclause}")
        # for bindings in json_response['bindings']:
        #  topk.append(bindings)
        # for e in reversed(expr):
        #     reverse = bool(e.order and e.order == 'DESC')
        #     topk = sorted(topk, key=lambda x: to_rdflib_term(x['?'+e.expr]),reverse=reverse)


    if output is not None:
        with open(output, 'w') as output_file:
            output_file.write(dumps(results))
    logger.info(f'\n{results}')
    # logger.info(f'{obj} : {max}')

    if measures is not None:
        with open(measures, 'w') as measures_file:
            avg_loading_time = mean(loading_times)
            avg_resume_time = mean(resume_times)
            measures_file.write(f'{query_name},{engine},{limit},{execution_time},{nb_calls},{nb_results},{avg_loading_time},{avg_resume_time}')
    logger.info(f'Query complete in {execution_time}s with {nb_calls} HTTP calls. {nb_results} solution mappings !')


if __name__ == "__main__":
    execute()
