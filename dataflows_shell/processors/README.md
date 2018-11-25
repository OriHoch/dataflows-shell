# DataFlows Shell Processors

Feel free to add additional processors as long as they don't add additional Python dependencies.

Having dependency on system executables is fine - free free to add processors for your favorite apps / tools.

## Core DataFlows Shell Processors

* `add_field` - Based on standard library add_field with more intuitive handling for dfs --fields argument

* `checkpoint` - Based on standard library checkpoint with support for named and autonumbered checkpoints

* `exec` - Execute a command from a field in each row (looks for `exec` field by default)

* `ls` - List files using a glob pattern

## Third party processors

* `kubectl` - Interact with [Kubernetes](https://kubernetes.io/) via `kubectl`

* `ckan` - Interact with the [CKAN Api](https://docs.ckan.org/en/2.8/api/index.html)
