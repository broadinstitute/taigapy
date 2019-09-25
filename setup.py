from setuptools import setup, find_packages
import sys

install_requires=['requests', 'pandas', 'boto3', 'colorful', 'progressbar2', 'feather-format']
if sys.version_info < (3, 5):
    install_requires.append('enum34')

import ast
import re
_version_re = re.compile(r'__version__\s*=\s*(.*)')
with open("taigapy/__init__.py", 'rt') as f:
    version = str(ast.literal_eval(_version_re.search(
        f.read()).group(1)))

setup(
    name='taigapy',
    version=version,
    packages=find_packages(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Remi Marenco",
    author_email="rmarenco@broadinstitute.org",
    long_description=open('README.md').read(),
    install_requires=install_requires
    )
