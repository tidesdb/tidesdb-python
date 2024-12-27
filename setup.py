from setuptools import setup, find_packages

setup(
    name="tidesdb",
    version="0.4.0",
    packages=find_packages(),
    author="TidesDB",
    author_email="hello@tidesdb.com",
    description="A Python wrapper-binding for TidesDB",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    classifiers=[
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.6',
    test_suite='tests'
)