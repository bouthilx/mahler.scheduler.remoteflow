# TODO: - Test install of mahler_test
#       - Test singularity container of mahler_test
#       - Try registering
#       - Use mahler_test to test deployment on graham, beluga and cedar using remoteflow.
import argparse

import mahler.client as mahler

from mahler_test.ops import run


def main(argv=None):

    parser = argparse.ArgumentParser(description='Script to register tests')

    parser.add_argument('-t', '--tags', nargs='*')
    # 'bouthilx/mahler.registry.mongodb'
    parser.add_argument('-c', '--container', type=str, default=None)
    parser.add_argument('-n', type=int, default=100, help='Number of tasks to register')

    options = parser.parse_args(argv)

    mahler_client = mahler.Client()

    for i in range(options.n):
        mahler_client.register(
            run.delay(dummy=i), tags=['test'] + options.tags,
            container=options.container)

    mahler_client.close()


if __name__ == "__main__":
    main()
