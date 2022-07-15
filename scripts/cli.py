import click
import json
import yaml
import os
import glob
import logging
import hashlib

from pandas import DataFrame
from typing import Tuple, List

from spy import Spy
from approaches.factory import ApproachFactory


###############################################################################
# ### Helping functions
###############################################################################


def list_files(path: str) -> List[str]:
    if not os.path.isdir(path):
        return glob.glob(path)
    files = list()
    for filename in os.listdir(path):
        if filename.endswith(".sparql"):
            files.append(f"{path}/{filename}")
    return files


def load_queries(path: str) -> List[Tuple[str, str]]:
    queries = list()
    for file in list_files(path):
        with open(file, 'r') as reader:
            filename = os.path.basename(file).split('.')[0]
            query = reader.read()
            queries.append((filename, query))
    return queries


def save_dataframe(dataframe: DataFrame, output: str, mode: str = 'w') -> None:
    if output is not None:
        header = not (mode == 'a' and os.path.exists(output))
        dataframe.to_csv(output, mode=mode, index=False, header=header)


def save_json(data: dict, output: str) -> None:
    if output is not None:
        with open(output, 'w') as outfile:
            json.dump(data, outfile, indent=4)


###############################################################################
# ### Command-line interface
###############################################################################


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "queryfile", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option(
    "--configfile",
    type=click.Path(exists=True, file_okay=True, dir_okay=False),
    default="config/xp.yaml")
@click.option(
    "--approach", type=click.Choice(ApproachFactory.types()), default="sage")
@click.option(
    "--limit", type=click.INT, default=10)
@click.option(
    "--quota", type=click.INT, default=None)
@click.option(
    "--force-order/--default-ordering", default=False)
@click.option(
    "--stats", type=click.Path(exists=False), default=None)
@click.option(
    "--output", type=click.Path(exists=False), default=None)
@click.option(
    "--verbose/--quiet", default=False)
def topk_run(
    queryfile, configfile, approach, limit, quota, force_order,
    stats, output, verbose
):
    if verbose:
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S")
    config = yaml.safe_load(stream=open(configfile, 'r'))
    filename, query = load_queries(queryfile)[0]

    spy = Spy(filename)  # used to collect statistics
    engine = ApproachFactory.create(approach, config)

    solutions = engine.execute_query(
        query, spy, limit=limit, quota=quota, force_order=force_order)
    dataframe = spy.to_dataframe()

    first_solutions = json.dumps(solutions[:10], indent=4)

    logging.info((
        f"{approach} - query executed in {spy.execution_time} seconds "
        f"with {len(solutions)} solutions"))
    logging.info(f"{approach} - first 10 solutions:\n{first_solutions}")
    logging.info(f"{approach} - dataframe:\n{dataframe}")

    save_json(solutions, output)
    save_dataframe(dataframe, stats)


@cli.command()
@click.argument(
    "reference", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument(
    "actual", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.option(
    "--output", type=click.Path(exists=False), default=None)
@click.option(
    "--verbose/--quiet", default=False)
def compare(reference, actual, output, verbose):
    if verbose:
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(message)s",
            datefmt="%m/%d/%Y %I:%M:%S")
    reference = json.load(open(reference, 'r'))
    actual = json.load(open(actual, 'r'))

    correct = True
    memory = {}
    for mappings in reference:
        sorted_mappings = {k: mappings[k] for k in sorted(mappings.keys())}
        key = hashlib.md5(str(sorted_mappings).encode('utf-8')).digest()
        if key not in memory:
            memory[key] = 0
        memory[key] += 1
    for mappings in actual:
        sorted_mappings = {k: mappings[k] for k in sorted(mappings.keys())}
        key = hashlib.md5(str(sorted_mappings).encode('utf-8')).digest()
        if key not in memory:
            logging.info(f"Incorrect solution: {sorted_mappings}")
            correct = False
            break
        memory[key] -= 1
        if memory[key] < 0:
            logging.info(f"Duplicated solution: {sorted_mappings}")
            correct = False
            break
    correct = all([value == 0 for value in memory.values()])

    if correct:
        logging.info("The TOP-K is correct")
    else:
        logging.info("The TOP-K is incorrect")
    DataFrame([[correct]], columns=["correct"]).to_csv(output, index=False)


if __name__ == "__main__":
    cli()
