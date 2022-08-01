# Processing SPARQL TOP-k Queries Online with Web Preemption

This repository contains the source code, the configuration files, the queries and the datasets used in the experimental study presented in the paper [Processing SPARQL TOP-k Queries Online with Web Preemption](...).

If you have any questions, feel free to contact the authors;
- Julien AIMONIER-DAVAT via julien.aimonier-davat@univ-nantes.fr
- Pascal MOLLI via pascal.molli@univ-nantes.fr
- Hala SKAF-MOLLI via hala.skaf@univ-nantes.fr

## Setup

To quickly get started, run the following commands on one machine which will install everything you need to reproduce our experimental results.

1. Clone and install the project.
    
    <details>
    <summary>Details</summary>
    <br>

    ```bash
    git clone https://github.com/momo54/sage-orderby-experiment.git topk
    cd topk

    conda env create -f environment.yml
    conda activate topk
    ```

    </details>


2. Install Virtuoso v7.2.6.
    
    <details>
    <summary>Details</summary>
    <br>

    ```bash
    wget https://github.com/openlink/virtuoso-opensource/releases/download/v7.2.6.1/virtuoso-opensource-7.2.6.tar.gz
    tar -zxvf virtuoso-opensource-7.2.6.tar.gz

    cd virtuoso-opensource-7.2.6
    ./configure
    make
    make install
    ```
  
    To run the experiments, the bin directory of Virtuoso must be defined in your PATH variable.

    </details>
    

3. Install SaGe.
    
    <details>
    <summary>Details</summary>
    <br>

    ```bash
    # In the main directory of the github repository
    git clone https://github.com/sage-org/sage-engine.git

    cd sage-engine
    git checkout topk-xp
    
    poetry install --extras "hdt"
    ```

    </details>
    

4. Download RDF datasets.
    
    <details>
    <summary>Details</summary>
    <br>

    ```bash
    # In the main directory of the github repository
    wget nas.jadserver.fr/thesis/xp/topk.tar.gz
    tar -zxvf topk.tar.gz
    ```

    </details>
    
    
5. Load data into Virtuoso

    <details>
    <summary>Details</summary>
    <br>

    ```bash
    isql "EXEC=ld_dir('datasets', '*.nt', 'http://example.com/datasets/default');"
    isql "EXEC=rdf_loader_run();"
    isql "EXEC=checkpoint;"
    ```
  
    </details>
    
Virtuoso installation can be skipped if your are not interesting in checking the correctness and completeness of query results.

## Quickstart

Experiments are powered by [snakemake](https://snakemake.readthedocs.io/en/stable), a scientific workflow management system in Python. Once all configuration files are defined, just run the following commands. Snakemake will generate an archive *xp.tar.gz* in the specified output directory. Data files in the generated archive can be loaded and visualized using the provided jupyter notebook.

```bash
snakemake --configfile config/xp-watdiv.yaml -j1

snakemake --configfile config/xp-wikidata.yaml -j1

jupyter notebook topk.jpynb
```

## Configuration files

Experiments are defined using YAML configuration files available in the [config](config) directory. The template of configuration files is the following:

```yaml
name: ... # the name of the configuration file
output: ... # output directory where data files will be generated
autostart: ... # True to let snakemake starts SaGe and Virtuoso servers, False otherwise
endpoints:
  sage:
    url: ... # URL of the SaGe endpoint
    graph: ... # IRI of an RDF graph
  virtuoso:
    url: # URL of the Virtuoso endpoint
    graph: # IRI of an RDF graph
experiments:
  xp_1: # a name for the experiment
    approaches: [...] # accepted values are "sage", "sage-topk" or "sage-partial-topk"
    workloads: [...] # accepted values are "watdiv", "watdiv-desc" or "wikidata"
    limits: [...] # tested k, i.e. number of results return by TOP-k queries
    runs: [...] # any identifier from 0 to 9 to differentiate each run. The mean of the runs will be computed later...
    quotas: [...] # tested quotas, i.e. duration of a quantum for SaGe
    stateless: ... # False to store query saved plans on the server, True otherwise
    early_pruning: ... # True to enable early-pruning, False otherwise
    max_limit: ... # limit K for the SaGe server
    check: ... # True to check query results using Virtuoso, False otherwise
  ...
  xp_n: ...
```