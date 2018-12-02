import sys
from requests_html import HTMLSession, Element
from dataflows import Flow


class html_requests(Flow):

    def __init__(self, action, url, query):
        self.action, self.url, self.query = action, url, query
        super().__init__()

    def _chain(self, ds=None):
        self.chain = self._html_requests(self.action, self.url, self.query),
        return super()._chain(ds)

    def _get_element_item(self, element):
        print("ERROR!")
        raise Exception('ERROR')

    def _html_requests(self, action, url, query):
        assert action == 'get', f'action is not supported: {action}'
        session = HTMLSession()
        r = session.get(url)
        res = eval(query)
        if isinstance(res, str):
            return ({'item': item} for item in res.split("\n"))
        elif isinstance(res, (set, list)):
            res = list(res)
            item_type = res[0].__class__.__name__
            if item_type == 'Element':
                return (self._get_element_item(item) for item in res)
            elif item_type == 'str':
                return ({"value": value} for value in res)
            else:
                raise Exception('item_type is not supported: ' + item_type)
        else:
            raise Exception('result type is not supported: ' + type(res))
