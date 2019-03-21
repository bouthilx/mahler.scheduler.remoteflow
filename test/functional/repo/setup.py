#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Installation script for functional test repo of `mahler.scheduler.remoteflow`."""
import os

from setuptools import setup


setup_args = dict(
    name='mahler_test',
    description='',
    long_description='',
    license='GNU GPLv3',
    author=u'Xavier Bouthillier',
    author_email='xavier.bouthillier@umontreal.ca',
    url='https://github.com/bouthilx/mahler.scheduler.remoteflow',
    packages=['mahler_test'],
    include_package_data=True,
    # data_files=find_data_files(),
    install_requires=['mahler.core'],
    setup_requires=['setuptools', 'pytest-runner>=2.0,<3dev']
    )


if __name__ == '__main__':
    setup(**setup_args)
