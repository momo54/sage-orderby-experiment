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
def execution_times(data, output):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[dataframe["limit"]==10]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="execution_time",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels("", "Queries")
    chart.legend.set_title("Execution time of queries")
    chart.set(yscale="log")

    chart.savefig(output)

@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def data_transfer(data, output):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[dataframe["limit"]==10]

    chart=sns.catplot(data=selection, kind="bar",x="query", y="nb_results",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(bottom=True)
    chart.set_axis_labels("", "Queries")
    chart.set(yscale="log")
    chart.legend.set_title("Data transfer")
#    plt.legend(loc='upper left')
    chart.savefig(output)

@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def nb_calls(data, output):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[dataframe["limit"]==10]

    chart=sns.catplot(data=selection, kind="bar",x="query", y="nb_calls",hue="engine",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels("queries", "nb calls")
    chart.legend.set_title("Number of calls")
    chart.set(yscale="log")

    chart.savefig(output)


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def change_limit(data, output):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[dataframe["engine"]=="orderby"]

    chart=sns.catplot(data=selection, kind="bar",x="query", y="execution_time",hue="limit",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels("times in s", "Queries")
    chart.legend.set_title("Changing limits")
    chart.set(yscale="log")

    chart.savefig(output)


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def change_limit_overhead(data, output):
    dataframe = read_csv(data, sep=',')
    selection=dataframe[dataframe["engine"]=="orderby"]
    selection['overhead']=selection["loading_time"]+selection["resume_time"]
    chart=sns.catplot(data=selection, kind="bar",x="query", y="overhead",hue="limit",
        palette="dark", alpha=.6, height=6)
    chart.despine(left=True)
    chart.set_axis_labels("times in s", "Queries")
    chart.legend.set_title("overhead")
    chart.set(yscale="log")

    chart.savefig(output)



@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def suspend_resume_times(data, output):
    dataframe = read_csv(data, sep=',')

    figure, axes = plt.subplots(1, 2)

    loading_times = sns.barplot(x="query", y="loading_time", hue="backend", data=dataframe, ax=axes[0])
    loading_times.get_legend().remove()
    loading_times.set_ylabel("Time to resume the query (ms)")
    loading_times.set_xlabel("")

    resume_times = sns.barplot(x="query", y="resume_time", hue="backend", data=dataframe, ax=axes[1])
    resume_times.get_legend().remove()
    resume_times.set_ylabel("Time to suspend the query (ms)")
    resume_times.set_xlabel("")

    figure.text(0.5, 0.01, 'Indexes used to evaluate the query', ha='center')

    handles, labels = axes[1].get_legend_handles_labels()
    figure.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1), fancybox=True, ncol=3)

    plt.tight_layout()
    plt.subplots_adjust(top=0.87)

    plt.show()
    figure.savefig(output)


@cli.command()
@click.argument('data', type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument('output', type=click.Path())
def spo_execution_times(data, output):
    dataframe = read_csv(data, sep=',')

    dataframe = dataframe.loc[dataframe["query"] == "SPO"]

    backends = dataframe["backend"].unique()
    colors = sns.color_palette("tab10", n_colors=len(backends))

    chart = sns.barplot(x="backend", y="execution_time", data=dataframe, palette=colors)
    chart.set_ylabel("Query execution time (sec)")
    chart.set_xlabel("")

    cmap = dict(zip(backends, colors))
    patches = [Patch(color=v, label=k) for k, v in cmap.items()]

    plt.legend(handles=patches, loc='upper center', bbox_to_anchor=(0.5, 1.16), fancybox=True, ncol=3)
    chart.set_xticklabels(
        [ "" for b in chart.get_xticklabels() ],
        rotation=90,
        horizontalalignment="center",
        fontweight="light",
        fontsize="large"
    )
    show_values_on_bars(chart, "s")

    plt.ylim(0, 1500)
    plt.tight_layout()
    plt.subplots_adjust(top=0.87)

    plt.show()
    chart.get_figure().savefig(output)


if __name__ == "__main__":
    cli()
