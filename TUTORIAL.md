
Run the `ls` processor to find all `.zip` files under the user's home directory

```
dfs > ls $HOME/'**/*.zip' --recursive=yes
saving checkpoint to: .dfs-checkpoints/__1
checkpoint saved: __1
{'count_of_rows': 150, 'bytes': 14253, 'hash': '5e74c0c8948c8785f4782c5e975e5839', 'dataset_name': None}
```

Print a sample of the data

```
dfs > printer --num_rows=1
saving checkpoint to: .dfs-checkpoints/__2
using checkpoint data from .dfs-checkpoints/__1
res_1:
#    path                                                                                                    path_type      file_size
     (string)                                                                                                (string)       (integer)
---  ------------------------------------------------------------------------------------------------------  -----------  -----------
1    /home/ori/datapackage_last_120_days_2017-06-23.zip                                                      file            58086508
2    /home/ori/spark-v2.2.2-b.zip                                                                            file           119807055
...
150  /home/ori/knesset-data-pipelines/data/slim_feb25_18.zip                                                 file            14507834
checkpoint saved: __2
{'count_of_rows': 150, 'bytes': 14353, 'hash': 'c04f575435e29bd3a6d258b36350d0ca', 'dataset_name': None}
```

Save to a named checkpoint

```
dfs > --dump=checkpoint:home_zip_files
```

Mark files which are over a specified size

```
dfs > 'lambda row: row.update(keep=row["path_type"] == "file" and row["file_size"] > 1024 * 1024 * 100)' --fields=+keep:boolean
```

Filter to include only the selected files and print the result

```
dfs > filter_rows --kwargs='{"equals":[{"keep":true}]}' --print
```

Load from the named checkpoint, modify and re-run the filter:

```
'lambda row: row.update(keep=row["path_type"] == "file" and row["file_size"] > 1024 * 1024 * 10)' \
 --fields=+keep:boolean --load=checkpoint:home_zip_files \
 | dfs filter_rows --kwargs='{"equals":[{"keep":true}]}' --print
```

The last command demonstrates some more advanced syntax of the DataFlows DSL:

* A single backslash `\` at end of line is used to run multi-line commands
* Pipes `|` can be used to chain commands. Commands after the pipe should be prefixed with `dfs`
* `--load` argument is used to load from a named checkpoint instead of the default which is to load from the last auto-saved checkpoint
* `--kwargs` argument is used to provide complex data structures to processors

You can also load data from remote sources using the standard library `load` processor:

```
dfs > load https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv -c -p
```

The `-c` and `-p` are shorthand flags for `--clear` and `--print` - to clear all existing checkpoints and print the output.

For more details about the standard library processors, see the [DataFlows Processors Documentation](https://github.com/datahq/dataflows/blob/master/PROCESSORS.md)

DataFlows shell commands can be grouped into a `.dfs` file

Create a file called `get_country_gdp.dfs` with the following content:

```
#!/usr/bin/env dfs

load https://raw.githubusercontent.com/datasets/gdp/master/data/gdp.csv
filter_rows --kwargs='{"equals":[{"Country Name":"'"${DFS_ARG1}"'"}]}'
--dump=checkpoint:${DFS_ARG2}
printer
```

Make the file executable: `chmod +x get_country_gdp.dfs`

Exit from dfs shell and run the following from the system shell to get gdp for Israel and save in a named checkpoint:

```
$ ./get_country_gdp.dfs Israel gdp-israel
```

The DataFlows shell is just a thin wrapper around the system's shell,
this allows to use environment variables or execute shell commands

All dfs commands are actually shell commands, prefixed with `dfs`,
run the following from the system's shell to list files and add a constant value field:

```
$ dfs ls -c --fields=+generator_details="$(uname -a)" -p
```

Some more advanced dfs and shell integration - executing a shell script for each row

```
$ rm -f large_files &&\
    dfs ls -c \
        | dfs 'lambda row: row.update(large_file=row["path_type"] == "file" and row["file_size"] > 500)' --fields=+large_file:boolean \
        | dfs filter_rows '--kwargs={"equals":[{"large_file":true}]}' '--fields=+exec=echo "{path}:{file_size}" >> large_files' \
        | dfs exec &&\
    cat large_files
```

import dfs processor specs as shell aliases

```
$ source <(dfs import load printer filter_rows)
```

run the load processor with --load=null to create a new package for each resource and save the result to an environment variable

```
$ EMMIES=$(load https://raw.githubusercontent.com/datahq/dataflows/master/data/emmy.csv --name=emmies --load=null -p)
$ OSCARS=$(load https://raw.githubusercontent.com/datahq/dataflows/master/data/academy.csv --encoding=utf8 --name=oscars --load=null -p)
```

load from the previously saved results and mark outstanding rows

```
$ OUTSTANDING_EMMIES=$(dfs 'lambda row: row.update(outstanding=row["category"] and "outstanding" in row["category"].lower())' \
    --fields=+outstanding:boolean --load=$EMMIES -p)
$ OUTSTANDING_OSCARS=$(dfs 'lambda row: row.update(outstanding=row["Award"] and "outstanding" in row["Award"].lower())' \
    --fields=+outstanding:boolean --load=$OSCARS -p)
```

load into a single package

```
$ load $(dfs get-checkpoint-path $OUTSTANDING_EMMIES)/datapackage.json --resources=emmies --load=null -p
$ load $(dfs get-checkpoint-path $OUTSTANDING_OSCARS)/datapackage.json --resources=oscars -p
```

filter and print the outstanding rows

```
$ filter_rows --args='[[{"outstanding":true}]]' -p
```
