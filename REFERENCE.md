## DataFlows Shell Reference

```
dfs [PROCESSOR_SPEC] [POSITIONAL_ARG..] [--NAMED_ARG=..]
```

**PROCESSOR_SPEC** - specifies the DataFlows processor to run from the following options:

* Name of a built-in dataflows-shell processor (defined under `dataflows_shell.processors`)
* Name of a built-in dataflows processor (defined under `dataflows.processors`)
* A lambda row procesing function: `lambda row: <ROW_PROCESSING_LAMBDA>` - for example: `'lambda row: row.update(interesting=row["avg_cpu_load"] > 90)'`
* Path to a file with extension `.dfs` - a DataFlows shell executable script
* A flow object or processor function in a python package - `my_module.my_processors:processor`

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
