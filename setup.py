"""
TidesDB Python Bindings Setup
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text() if (this_directory / "README.md").exists() else ""

setup(
    name="tidesdb",
    version="1.0.0",
    author="TidesDB Authors",
    author_email="me@alexpadula.com",
    description="Official Python bindings for TidesDB v1+",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tidesdb/tidesdb-python",
    project_urls={
        "Bug Tracker": "https://github.com/tidesdb/tidesdb-python/issues",
        "Documentation": "https://github.com/tidesdb/tidesdb",
        "Source Code": "https://github.com/tidesdb/tidesdb-python",
        "Discord": "https://discord.gg/tWEmjR66cy",
    },
    packages=find_packages(),
    py_modules=["tidesdb"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.7",
    install_requires=[
        # No external dependencies - uses ctypes with system library
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
    },
    keywords="database, key-value, lsm-tree, embedded, storage-engine",
    license="MPL-2.0",
)