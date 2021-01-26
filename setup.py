from setuptools import setup, find_packages
import sys

install_requires = [
    "requests",
    "pandas>=1.0.0",
    "aiobotocore==1.2.0",
    "colorful",
    "progressbar2>=3.3.0",
    "pyarrow>=2.0.0",
]
if sys.version_info < (3, 5):
    install_requires.append("enum34")

import ast
import re

_version_re = re.compile(r"__version__\s*=\s*(.*)")
with open("taigapy/__init__.py", "rt") as f:
    version = str(ast.literal_eval(_version_re.search(f.read()).group(1)))

setup(
    name="taigapy",
    version=version,
    packages=find_packages(),
    license="Creative Commons Attribution-Noncommercial-Share Alike license",
    author="Remi Marenco",
    author_email="rmarenco@broadinstitute.org",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    scripts=["bin/taigaclient"],
    install_requires=install_requires,
)
