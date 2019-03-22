import random

import mahler.client as mahler


@mahler.operator(resources={'gpu': 1, 'cpu': 1, 'mem': '100MB'})
def run(dummy=None):
    print('Hello world!')
    return dict(objective=random.random(), dummy=dummy), dict()
