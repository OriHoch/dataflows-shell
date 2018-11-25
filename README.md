# DataFlows Shell

DataFlows is a *"novel and intuitive way of building data processing flows."*

DataFlows Shell leverage DataFlows to use the same intuitive data processing flows for shell automation.

## Introduction

A lot of the work on the shell, especially for "DevOps" / automation type work, deals with data processing.
The first command a shell user learns is `ls` - which returns a set of data.
The second might by `grep` or `cp` - which filters and performs actions based on this data set.

DataFlows Shell acts as a very minimal and intuitive layer between the shell and [the DataFlows framework](https://github.com/datahq/dataflows).

## Demo

The following example demonstrates importing some processors to the local shell, using them to run a processor chain and printing the output. It uses the `kubectl` processor to get a list of pods from a Kubernetes cluster and filter based on a condition defined using a Python lambda function.

```
$ source <(dfs import printer filter_rows kubectl)

$ kubectl get pods -c -q \
    | dfs 'lambda row: row.update(is_ckan="ckan" in str(row["volumes"]))' --fields=+is_ckan:boolean -q
    | filter_rows --args='[[{"is_ckan":true}]]' -q

{'count_of_rows': 12, 'bytes': 57584, 'hash': '5febe0c3cfe75d174e242f290f00c289', 'dataset_name': None}
checkpoint:1
{'count_of_rows': 12, 'bytes': 57876, 'hash': '17f446a8f562f10cccc1de1a33c48d91', 'dataset_name': None}
checkpoint:2
{'count_of_rows': 6, 'bytes': 40797, 'hash': '6ab4290efd82478b1677d1f226c4199a', 'dataset_name': None}
checkpoint:3

$ printer --kwargs='{"fields":["kind","name","namespace"]}'

saving checkpoint to: .dfs-checkpoints/__9
using checkpoint data from .dfs-checkpoints/__8
res_1:
  #  kind        name                          namespace
     (string)    (string)                      (string)
---  ----------  ----------------------------  -----------
  1  Pod         ckan-5d74747649-92z9x         odata-blue
  2  Pod         ckan-5d74747649-fzvd6         odata-blue
  3  Pod         ckan-jobs-5d895695cf-wgrzr    odata-blue
  4  Pod         datastore-db-944bfbc74-2nc7b  odata-blue
  5  Pod         db-7dd99b8547-vpf57           odata-blue
  6  Pod         pipelines-9f4466db-vlzm8      odata-blue
checkpoint saved: __9
{'count_of_rows': 6, 'bytes': 40798, 'hash': 'adc31744dfc99a0d8cbe7b081f31d78b', 'dataset_name': None}
checkpoint:9
```

## Install

The only required core dependencies are Bash and Python3.7+

To get a compatible Python you can use [Miniconda](https://conda.io/miniconda.html):

```
$ wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
$ bash Miniconda3-latest-Linux-x86_64.sh
$ wget https://raw.githubusercontent.com/OriHoch/dataflows-shell/master/environment.yaml
$ conda env create -f environment.yaml
$ conda activate dataflows-shell
```

Install the dataflows-shell package

```
python3 -m pip install -U dataflows-shell
```

Start an interactive DataFlows shell session

```
$ dfs

DataFlows Shell

press <CTRL + C> to exit the shell
press <Enter> to switch between DataFlows shell and system shell
type '--help' for the DataFlows Shell reference

dfs > 
```

## Documentation

* [DataFlows Shell Tutorial](TUTORIAL.md)
* [DataFlows Shell Reference](REFERENCE.md)
* [DataFlows Shell Processors Reference](dataflows_shell/processors/README.md)
* [DataFlows Processors Reference](https://github.com/datahq/dataflows/blob/master/PROCESSORS.md)
