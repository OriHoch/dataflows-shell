from dataflows.helpers.resource_matcher import ResourceMatcher


def column_adder(rows, k, v):
    for rownum, row in enumerate(rows):
        if k not in row:
            if v:
                row[k] = v.format(**dict(row, __rownum=rownum))
            else:
                row[k] = v
        yield row


def add_field(name, type, default=None, resources=None, **options):

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                if name not in [f['name'] for f in resource['schema']['fields']]:
                    resource['schema']['fields'].append(dict(
                        name=name,
                        type=type,
                        **options
                    ))
        yield package.pkg
        for res in package:
            if matcher.match(res.res.name):
                yield column_adder(res, name, default)
            else:
                yield res

    return func
