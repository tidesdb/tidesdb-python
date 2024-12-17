from setuptools import setup, find_packages

setup(
    name="tidesdb",  
    version="0.1",  
    packages=find_packages(), 
    author="Alex Gaetano Padula",
    author_email="me@alexpadula.com",
    description="A Python wrapper-binding for TidesDB",
    long_description=open("README.md").read(),  
    long_description_content_type="text/markdown", 
    classifiers=[ 
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    python_requires='>=3.6', 
)