import dataflows_shell.processors
import dataflows.processors
from importlib import import_module


def is_valid_built_in_processor_spec(processor_spec):
    for c in ['_', '/', '.']:
        if processor_spec.startswith(c):
            return False
    if ' ' in processor_spec:
        return False
    return True


def parse_processor_spec(processor_spec):
    if processor_spec.startswith('lambda row: '):
        def _lambda_func(*args, **kwargs):
            return eval(processor_spec)
        return _lambda_func
    elif is_valid_built_in_processor_spec(processor_spec):
        if hasattr(dataflows_shell.processors, processor_spec):
            return getattr(dataflows_shell.processors, processor_spec)
        elif hasattr(dataflows.processors, processor_spec):
            return getattr(dataflows.processors, processor_spec)
    if ':' in processor_spec:
        module_spec, processor_spec = processor_spec.split(':')
        flow_module = import_module(module_spec)
        return getattr(flow_module, processor_spec)
    else:
        raise Exception(f'Invalid processor spec: {processor_spec}')
