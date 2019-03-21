from mahler.client import Client

from ops import run


mahler = Client()


tags = ['examples', 'random', 'remoteflow', 'v1.0']


for i in range(100):
    mahler.register(run.delay(dummy=i), tags=tags, container='shallow2.0')
