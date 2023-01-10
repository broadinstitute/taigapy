from setuptools import setup, find_packages

install_requires = [
    "requests",
    "pandas>=1.0.0,<2.0.0",
    "aiobotocore==1.2.2",
    "boto3>=1.16.0,<1.16.53",
    "nest_asyncio>=1.5.1,<2.0.0",
    "colorful",
    "progressbar2>=3.3.0,<4.0.0",
    "pyarrow>=3.0.0",
]

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
