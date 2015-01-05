import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='stackdistiller',
    version='0.11',
    author='Monsyne Dragon',
    author_email='mdragon@rackspace.com',
    description=("A data extraction and transformation library for "
                 "OpenStack notifications"),
    license='Apache License (2.0)',
    keywords='OpenStack notifications events extraction transformation',
    packages=find_packages(exclude=['tests']),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    url='https://github.com/stackforge/stacktach-stackdistiller',
    scripts=['bin/test-distiller.py'],
    long_description=read('README.md'),
    install_requires=[
        "enum34 >= 1.0",
        "iso8601 >= 0.1.10",
        "jsonpath-rw >= 1.2.0, < 2.0",
        "PyYAML >= 3.1.0",
        "six >= 1.5.2",
    ],

    zip_safe=False
)
