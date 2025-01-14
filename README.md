## TidesDB Python

Official Python Binding for TidesDB.

### Overview

This package provides Python bindings for TidesDB, allowing you to seamlessly integrate TidesDB's powerful features into your Python applications. The bindings are built on top of the TidesDB C foundation, which is automatically set up during installation.

### Requirements

- Python 3.6 or higher
- Git
- CMake 3.15 or higher
- C++ compiler


### Installation

We provide automated setup scripts for both Linux/macOS and Windows environments.

#### Linux/macOS

```bash
# Clone the repository
git clone https://github.com/tidesdb/tidesdb-python
cd tidesdb-python

# Run the setup script
./setup.sh

# Activate the virtual environment
source venv/bin/activate  # Your prompt should show (venv) at the beginning
```

#### Windows

```powershell
# Clone the repository
git clone https://github.com/tidesdb/tidesdb-python
cd tidesdb-python

# Run the setup script
.\setup.ps1

# Activate the virtual environment
# For Windows (PowerShell or Command Prompt):
venv\Scripts\activate  # Your prompt should show (venv) at the beginning
```

### Understanding the Setup Process

The installation process consists of several important steps:

1. **C Foundation Setup**
   - The setup scripts automatically clone and configure the TidesDB C foundation (`cfoundation/`).
   - This component is crucial as it provides the core database functionality that the Python bindings interact with.
   - The scripts handle:
     - Cloning the C foundation repository (only essential files)
     - Configuring build options for optimal performance
     - Building and installing the C library
     - Integrating it seamlessly with the Python bindings

2. **Python Environment**
   - The setup script automatically:
     - Creates a virtual environment for isolated package management
     - Installs all required Python dependencies
     - Sets up the Python package in development mode
   - After installation, you must manually activate the virtual environment:
     - For Linux/macOS: `source venv/bin/activate`
     - For Windows: `venv\Scripts\activate`
     - Once activated, your prompt should show (venv) at the beginning

#### Automated Setup Details

The setup scripts (`setup.sh` and `setup.ps1`) automate several complex tasks:

- **C Foundation Configuration**
  - Disables sanitizer and tests in the C foundation for production use
  - Optimizes build settings for release mode
  - Ensures proper integration with the Python bindings

- **Python Setup**
  - Creates an isolated virtual environment
  - Installs dependencies from `requirements.txt`
  - Sets up the package in development mode for easier testing and development

### Project Structure

```
tidesdb-python/
├── cfoundation/         # TidesDB C foundation (automatically set up)
├── samples/            # Example usage and tutorials
├── tests/             # Test suite
├── tidesdb/           # Main Python package
├── venv/              # Python virtual environment (created during setup)
├── requirements.txt   # Python dependencies
├── setup.py          # Package installation script
├── setup.ps1         # Windows setup script
└── setup.sh          # Linux/macOS setup script
```


### Usage

After installation, you can start using TidesDB in your Python applications. We provide several examples to help you get started.

#### Basic Operations Example

The `samples/basic_operations.py` demonstrates fundamental TidesDB operations. To run the example:

```bash
# Navigate to the samples directory
cd samples

# Run the basic operations example
python basic_operations.py
```

This script demonstrates:
- Database initialization and configuration
- Column family creation with optimized settings
- Basic key-value operations:
  - Putting data (`db.put`)
  - Retrieving data (`db.get`)
  - Deleting data (`db.delete`)
- Proper resource cleanup and error handling

Example output:
```
2024-01-13 10:00:00,000 - INFO - Opening database at: /path/to/basic_output
2024-01-13 10:00:00,001 - INFO - Creating column family: default_cf
2024-01-13 10:00:00,002 - INFO - Putting key-value: b"user:1" -> b"John Doe"
2024-01-13 10:00:00,003 - INFO - Retrieved value: b"John Doe"
2024-01-13 10:00:00,004 - INFO - Deleting key: b"user:1"
2024-01-13 10:00:00,005 - INFO - Key successfully deleted
2024-01-13 10:00:00,006 - INFO - All basic operations completed successfully!
```

#### Cursor Operations Example

The `samples/cursor_operations.py` demonstrates how to use cursors for iterating over data in TidesDB. To run the example:

```bash
cd samples
python cursor_operations.py
```

This script showcases:
- Cursor initialization and management
- Forward iteration through key-value pairs
- Proper cursor resource handling
- Cursor behavior with database modifications

Example output:
```
2024-01-13 10:00:00,000 - INFO - Opening database at: /path/to/cursor_output
2024-01-13 10:00:00,001 - INFO - Creating column family: cursor_test_cf
2024-01-13 10:00:00,002 - INFO - Iterating through entries:
2024-01-13 10:00:00,003 - INFO - Key: b"key1" -> Value: b"value1"
2024-01-13 10:00:00,004 - INFO - Key: b"key2" -> Value: b"value2"
2024-01-13 10:00:00,005 - INFO - Cursor operations completed successfully!
```

#### Transaction Operations Example

The `samples/transaction_operations.py` demonstrates atomic transactions in TidesDB. To run the example:

```bash
cd samples
python transaction_operations.py
```

This script illustrates:
- Transaction creation and management
- Atomic operations within transactions
- Transaction commit and rollback functionality
- Transaction isolation properties

Example output:
```
2024-01-13 10:00:00,000 - INFO - Opening database at: /path/to/transaction_output
2024-01-13 10:00:00,001 - INFO - Starting transaction
2024-01-13 10:00:00,002 - INFO - Performing atomic operations
2024-01-13 10:00:00,003 - INFO - Transaction committed successfully
2024-01-13 10:00:00,004 - INFO - Verifying transaction results
```

### Testing

The project includes a comprehensive test suite covering all major functionality:

#### Running Tests

You can run the entire test suite:

```bash
python -m unittest discover tests
```

Or run specific test files:

```bash
# Basic operations tests
python -m unittest tests/test_basic_operations.py

# Cursor operations tests
python -m unittest tests/test_cursor.py

# Transaction operations tests
python -m unittest tests/test_transaction.py
```

#### Test Structure

- `tests/test_basic_operations.py`: Tests for fundamental database operations
  - Put/Get operations
  - Delete operations
  - Multiple operations sequence

- `tests/test_cursor.py`: Tests for cursor functionality
  - Forward iteration
  - Cursor behavior with database modifications
  - Resource management

- `tests/test_transaction.py`: Tests for transaction handling
  - Successful transaction completion
  - Transaction rollback
  - Transaction isolation

Each test file includes detailed logging for better visibility into the test execution process.

Example test output:
```
2024-01-13 10:00:00,000 - INFO - Setting up test environment
2024-01-13 10:00:00,001 - INFO - Running test_put_and_get
2024-01-13 10:00:00,002 - INFO - Test completed successfully
...
----------------------------------------------------------------------
Ran 8 tests in 0.025s
OK
```

## Development

For development, the package is installed in editable mode (`pip install -e .`), allowing you to modify the code and see changes immediately without reinstallation.

## License

This project is licensed under the Mozilla Public License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contributing

We welcome contributions! Please see our contributing guidelines for more details.