from setuptools import setup, find_packages
import sys

install_requires=['requests', 'pandas', 'boto3']
if sys.version_info < (3, 5):
    install_requires.append('enum34')

setup(
    name='taigapy',
    version='2.0.0',
    packages=find_packages(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomery",
    author_email="pmontgom@broadinstitute.org",
    long_description=open('README.md').read(),
    install_requires = install_requires
    )