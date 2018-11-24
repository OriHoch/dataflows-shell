# DataFlows Shell

DataFlows Shell enhances [DataFlows](https://github.com/datahq/dataflows) with shell integration.

## Quickstart

Install

```
$ pip install dataflows-shell
```

Start the DataFlows shell

```
$ dfs

dfs >
```
<!-- start-cli-help-message -->

## Usage Examples

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

## DataFlows Shell Reference

```
dfs [PROCESSOR_SPEC] [POSITIONAL_ARG..] [--NAMED_ARG=..]
```

**PROCESSOR_SPEC** - specifies the DataFlows processor to run

The following parsing attempts are made, first one that matches to a processor is used:

* Name of a built-in dataflows-shell processor (defined under `dataflows_shell.processors`)
* Name of a built-in dataflows processor (defined under `dataflows.processors`)
* A lambda row procesing function: `lambda row: <ROW_PROCESSING_LAMBDA>` - for example: `'lambda row: row.update(interesting=row["avg_cpu_load"] > 90)'`
* Path to a file with extension `.dfs` - a DataFlows shell executable script

**POSITIONAL_ARG** - positional args are passed on to the processor function positional args as integers or strings

**NAMED_ARG** - named arguments in the format `--arg_name=arg_value` or `-short_arg=value`

The following named args are handled by dfs, all other args are passed on to the processor:

* `-l=<LOAD_SPEC>` `--load=<LOAD_SPEC>` - Load the resource/s from the provided LOAD_SPEC before running the processor
  * The LOAD_SPEC is parsed according to the following rules, first one that matches is used:
  * `null` - Don't load anything
  * `checkpoint` - Loads from the last auto-numbered checkpoint, this is the default if no load argument is provided
  * `checkpoint:<INTEGER>` - Load from a checkpoint number
  * `checkpoint:<NAME>` - Load from a named checkpoint
  * `<DATAPACKAGE_PATH>` - A path in local filesystem containing a datapackage.json file

* `-d=<DUMP_SPEC>` `--dump=<DUMP_SPEC>` - Dump the output to the provided DUMP_SPEC
  * The DUMP_SPEC is parsed according to the following rules, first one that matches is used:
  * `null` - Don't dump anything
  * `checkpoint` - Dumps to an auto-numbered checkpoint, this is the default if no dump argument is provided
  * `checkpoint:<NAME>` - Dump to a named checkpoint
  * `<DATAPACKAGE_PATH>` - Dump to the given path

* `-a=<PROCESSOR_ARGS_JSON>` `--args=<PROCESSOR_ARGS_JSON>` - Json list containing processor positional args

* `-k=<PROCESSOR_KWARGS_JSON>` `--kwargs=<PROCESSOR_KWARGS_JSON>` - Json dict containing processor named args

* `-f=<PRINT_FIELDS>` `--print-fields=<PRINT_FIELDS>` - Must be used with `--print` - limits the printing to the specified comma-separated list of field names

* `-o=<PRINT_FORMAT>` `--print-format=<PRINT_FORMAT>` - specifies the output format
  * `text` - The default format which prints a textual preview of the first 2 rows and the last row
    * When using this format the text is printed to stderr to support chaining dfs processors
  * `json` / `yaml` - Dumps a list in the required format containing the output from DataFlows Flow.results() method
    * List of resourcs, each resource is a list of rows, each row is a key-value map
    * Datapackage schema
    * Stats

* `--fields=<FIELD_SPEC>,..` - Modify field schema based on the comma-separated list of field specs -
  * Change type for an existing field: `<FIELD_NAME>:<FIELD_TYPE>`
  * Delete a field: `-<FIELD_NAME>`
  * Add a string field: `+<FIELD_NAME>`
  * Add a field of the provided type: `+<FIELD_NAME>:<FIELD_TYPE>`
  * Add a string field with the provided value: `+<FIELD_NAME>=<VALUE>`

* `-ls` `--list-checkpoints` - List the named checkpoints

* `-c` `--clear` - Clear the auto-numbered checkpoints (only works when loading from a checkpoint)

* `-p` `--print` - Print the output using the standard DataFlows printer processor

* `-q` `--quiet` - Don't print any output

## DataFlows Shell Processors Reference

The following processors are available from dataflows-shell in addition to the [DataFlows Processors](https://github.com/datahq/dataflows/blob/master/PROCESSORS.md)

* `add_field` - Based on standard library add_field with more intuitive handling for dfs --fields argument

* `checkpoint` - Based on standard library checkpoint with support for named and autonumbered checkpoints

* `exec` - Execute a command from a field in each row (looks for `exec` field by default)

* `ls` - List files using a glob pattern

<!-- end-cli-help-message -->
