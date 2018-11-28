from dataflows import set_type, delete_fields
from .processors import add_field


def get_dfs_fields_steps(dfs_kwargs):
    steps = []
    if 'fields' in dfs_kwargs:
        fields_to_delete = set()
        for field in dfs_kwargs['fields'].split(','):
            field = field.strip()
            if field.startswith('+'):
                if ':' in field:
                    tmpfield = field[1:].split(':')
                    field_name = tmpfield[0]
                    field_type = ':'.join(tmpfield[1:])
                    if field_type in ['string', 'integer', 'date', 'number', 'boolean']:
                        steps.append(add_field(field_name, field_type))
                        continue
                field = field[1:].split('=')
                field_name = field[0]
                field_value = '='.join(field[1:]) if len(field) > 1 else ''
                steps.append(add_field(field_name, 'string', field_value))
            elif field.startswith('-'):
                fields_to_delete.add(field[1:])
            else:
                field = field.split(':')
                field_name = field[0]
                field_type = field[1] if len(field) > 1 else 'string'
                steps.append(set_type(field_name, type=field_type))
        if len(fields_to_delete) > 0:
            steps.append(delete_fields(list(fields_to_delete)))
    return tuple(steps)
