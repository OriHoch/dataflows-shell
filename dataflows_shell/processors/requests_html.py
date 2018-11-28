import sys
from requests_html import HTMLSession, Element
from dataflows import Flow


class requests_html(Flow):

    def __init__(self, action, url, query):
        self.action, self.url, self.query = action, url, query
        super().__init__()

    def _chain(self, ds=None):
        self.chain = self._requests_html(self.action, self.url, self.query),
        return super()._chain(ds)

    def _get_element_item(self, element):
        print("ERROR!")
        raise Exception('ERROR')

    def _requests_html(self, action, url, query):
        assert action == 'get', f'action is not supported: {action}'
        session = HTMLSession()
        r = session.get(url)
        res = eval(query)
        if isinstance(res, str):
            return ({'item': item} for item in res.split("\n"))
        elif isinstance(res, (set, list)):
            item_type = res[0].__class__.__name__
            if item_type == 'Element':
                return (self._get_element_item(item) for item in res)
            else:
                raise Exception('item_type is not supported: ' + item_type)
        else:
            raise Exception('result type is not supported: ' + type(res))
