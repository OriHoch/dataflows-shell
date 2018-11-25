from dataflows import Flow


class my_flow(Flow):

    def __init__(self, foo, bar, num):
        super().__init__(self._foo_bar_generator(foo, bar, num))

    def _foo_bar_generator(self, foo, bar, num):
        for i in range(num):
            yield {'foo': foo, 'bar': bar}
