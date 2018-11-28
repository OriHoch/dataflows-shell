from dataflows import Flow
from collections import defaultdict
import json


def log_input(data=None, msg=None):
    if data:
        print(data)
    if msg:
        return input(f'~| {msg}')


def print_context(context):
    print(context)


def debug_row(row):
    return row


def debug_rows(rows):
    for rownum, row in enumerate(rows):
        yield debug_row(rownum, row)


def dfs_step_debugger(context, step):

    def debug_step(pos):
        step_context = dict(**context)
        step_context.update(pos=pos)

        def _debug_step(package):
            package_context = dict(**step_context)
            package_context.update(**{'package_name': package.pkg.descriptor.get('name', ''),
                                      'num_resources': len(package.pkg.descriptor.get('resources', []))})
            print_context(package_context)
            yield package.pkg
            for rows in package:
                rows_context = dict(**package_context)
                rows_context.update(resource_name=rows.res.name)
                print_context(rows_context)
                yield debug_rows(rows)

        return _debug_step

    if '-pre' in context['preprocess']:
        return Flow(debug_step('pre'), step)
    elif '-both' in context['preprocess']:
        return Flow(debug_step('pre'), step, debug_step('post'))
    else:
        return Flow(step, debug_step('post'))


class DfsDebuggerFlow(Flow):

    def __init__(self, args, context=None):
        self.context = context or {}
        super().__init__(*args)

    def _preprocess_chain(self):
        chain_context = dict(**self.context)
        if 'preprocess' not in chain_context:
            print('choose how to debug the chain pre-processing:')
            print('"all" steps should be debugged after the step')
            print('  "all-pre" steps should be debugger before the step')
            print('  "all-both" steps should be debugger before and after the step')
            print('"ask" how to debug each step - debugging for all steps will be before the step')
            print('  "ask-pre" ask and debug before the step')
            print('  "ask-both" ask and debug before and after the step')
            print('"skip" debugging for all steps')
            chain_context['preprocess'] = log_input(msg='Type one of the quoted words to make your selection [ask]: ')
            if chain_context['preprocess'] == '':
                chain_context['preprocess'] = 'ask'
        if chain_context['preprocess'] == 'skip':
            return super()._preprocess_chain()
        else:
            checkpoint_links = []
            for stepnum, link in enumerate(self.chain):
                if hasattr(link, 'handle_flow_checkpoint'):
                    checkpoint_links = link.handle_flow_checkpoint(checkpoint_links)
                else:
                    if isinstance(link, Flow):
                        subflow_context = dict(**chain_context)
                        subflow_context.update(subflow_level=subflow_context.get('subflow_level', 0) + 1,
                                               subflow_parent_stepnum=stepnum)
                        link = DfsDebuggerFlow(link.chain, subflow_context)
                    else:
                        step_context = dict(**chain_context, stepnum=stepnum,
                                            processor=type(link).__module__ + '.' + type(link).__qualname__)
                        if (step_context['preprocess'].startswith('all') or
                                log_input(step_context, 'Debug? (Y/n) ') in ['', 'Y', 'y', 'yes']
                        ):
                            link = dfs_step_debugger(step_context, link)
                    checkpoint_links.append(link)
            return checkpoint_links
