import seaborn as sns
import sys
import pandas
import click
import matplotlib.pyplot as plt

from pandas import read_csv
from matplotlib.patches import Patch


def show_values_on_bars(ax, unit):
    for p in ax.patches:
        ax.annotate(f"%d{unit}" % p.get_height(), (p.get_x() + p.get_width() / 2., p.get_height()),
            ha='center', va='center', fontsize=11, color='gray', xytext=(0, 20),
            textcoords='offset points')


@click.group()
def cli():
    pass


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
@click.option("--limit", type=int, default=10,
    help="Limit of a of SPARQL query.")
@click.option("--workload", type=str, default="watdiv",
    help="Which workload.")
def execution_times(data,  output,workload="watdiv", limit="10"):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[(dataframe["limit"]==limit) & (dataframe["workload"]==workload)]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="execution_time",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels( f"{workload} Queries with limit={limit}","execution time in s")
    chart.legend.set_title(f"engines")
    chart.set(yscale="log")

    chart.savefig(output)

@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
@click.option("--limit", type=int, default=10,
    help="Limit of a of SPARQL query.")
@click.option("--workload", type=str, default="watdiv",
    help="Which workload.")
def data_transfer(data,  output,workload, limit):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[(dataframe["limit"]==limit) & (dataframe["workload"]==workload)]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="nb_results",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(bottom=True)
    chart.set_axis_labels(f"{workload} Queries with limit={limit}","#mappings transfered")
    chart.set(yscale="log")
    chart.legend.set_title(f"engines")
#    plt.legend(loc='upper left')
    chart.savefig(output)

@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
@click.option("--limit", type=int, default=10,
    help="Limit of a of SPARQL query.")
@click.option("--workload", type=str, default="watdiv",
    help="Which workload.")
def nb_calls(data,output, limit,workload):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[(dataframe["limit"]==limit) & (dataframe["workload"]==workload)]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="nb_calls",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels(f"{workload} queries, limit={limit}","nb calls")
    chart.legend.set_title(f"engines")
    chart.set(yscale="log")

    chart.savefig(output)


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
@click.option("--workload", type=str, default="watdiv",
    help="Which workload.")
@click.option("--engine", type=str, default="orderby",
    help="Which engine.")
def change_limit(data,output, workload, engine):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[(dataframe["engine"]==engine) & (dataframe["workload"]==workload)]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="execution_time",hue="limit",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels(f"{workload} Queries",f"execution times in s for {engine} engines")
    chart.legend.set_title("limits")
    chart.set(yscale="log")

    chart.savefig(output)


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
@click.option("--workload", type=str, default="watdiv",
    help="Which workload.")
@click.option("--engine", type=str, default="orderby",
    help="Which engine.")
def change_limit_overhead(data, output, workload, engine):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[(dataframe["engine"]==engine) & (dataframe["workload"]==workload)]
    selection['overhead']=selection["loading_time"]+selection["resume_time"]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="overhead",hue="limit",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels(f"{workload} Queries",f"overhead times in s for {engine}")
    chart.legend.set_title("limits")
    chart.set(yscale="log")

    chart.savefig(output)

if __name__ == "__main__":
    cli()
