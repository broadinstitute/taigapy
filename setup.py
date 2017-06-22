from setuptools import setup, find_packages

setup(
    name='taigapy',
    version='2.0.0',
    packages=find_packages(),
    license='Creative Commons Attribution-Noncommercial-Share Alike license',
    author="Philip Montgomery",
    author_email="pmontgom@broadinstitute.org",
    long_description=open('README.md').read(),
    install_requires=['requests', 'pandas', 'boto3']
    )