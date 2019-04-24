#!/usr/bin/env python

from setuptools import setup, find_packages

import mapr_manager

setup(name='mapr_manager',
      version=mapr_manager.__version__,
      include_package_data=True,
      packages=find_packages(),
      description='MapR Cluster management utilities',
      url='https://github.com/remiforest/mapr-manager',
      author='Remi Forest',
      author_email='remi.forest@akilio.com',
      license='LICENSE.txt',
    )