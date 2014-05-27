import os
from pip.req import parse_requirements
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


req_file = os.path.join(os.path.dirname(__file__), "requirements.txt")
install_reqs = [str(r.req) for r in parse_requirements(req_file)]


setup(
    name='stackdistiller',
    version='0.10',
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
    url='https://github.com/StackTach/stackdistiller',
    scripts=['bin/test-distiller.py'],
    long_description=read('README.md'),
    install_requires=install_reqs,

    zip_safe=False
)
