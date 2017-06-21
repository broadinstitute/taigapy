from setuptools import setup, find_packages

setup(
    name='taigapy',
    version='1.0.1',
    packages=find_packages(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomery",
    author_email="pmontgom@broadinstitute.org",
    long_description=open('README.md').read(),
    install_requires=['requests', 'pandas', 'boto3']
    )