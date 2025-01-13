#!/bin/bash

set -e  # Stop the script in case of error

# Define C library repository
CLIB_REPO="https://github.com/tidesdb/tidesdb"

# Clone and set up C foundation
echo "Cloning TidesDB C foundation..."
git clone "$CLIB_REPO" cfoundation
cd cfoundation

# Remove .git directory as it will be part of the main project
echo "Cleaning up git history..."
rm -rf .git

# Configure build options
echo "Configuring build settings..."
sed -i.bak 's/option(TIDESDB_WITH_SANITIZER "build with sanitizer in tidesdb" ON)/option(TIDESDB_WITH_SANITIZER "build with sanitizer in tidesdb" OFF)/' CMakeLists.txt
sed -i.bak 's/option(TIDESDB_BUILD_TESTS "enable building tests in tidesdb" ON)/option(TIDESDB_BUILD_TESTS "enable building tests in tidesdb" OFF)/' CMakeLists.txt

# Build and install
echo "Building and installing C foundation..."
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
sudo cmake --install build

cd ..

# Setup Python environment in root directory
echo "Creating Python environment..."
python3 -m venv venv
source venv/bin/activate

# Install dependencies and package
echo "Installing dependencies..."
pip install -r requirements.txt
echo "Installing Python package..."
pip install -e .

echo -e "\nSetup complete! TidesDB Python is ready to use!"