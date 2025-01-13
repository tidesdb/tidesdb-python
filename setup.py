from setuptools import setup, find_packages

setup(
    name="tidesdb",
    version="0.5.0",
    packages=find_packages(include=['tidesdb', 'tidesdb.*']),
    author="TidesDB",
    author_email="hello@tidesdb.com",
    description="A Python wrapper-binding for TidesDB",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    ],
    python_requires='>=3.8',
    test_suite='tests'
)