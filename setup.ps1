# Stop on first error
$ErrorActionPreference = "Stop"

# Define C library repository
$CLIB_REPO = "https://github.com/tidesdb/tidesdb"

# Clone with sparse checkout
Write-Host "Cloning TidesDB C foundation (minimal)..."
git clone --filter=blob:none --sparse $CLIB_REPO cfoundation
Set-Location cfoundation

# Configure sparse checkout patterns
git sparse-checkout set --no-cone '/*' '!artwork/' '!test/' '!.github/' '!CODE_OF_CONDUCT.md' '!CONTRIBUTING.md' '!LICENSE' '!README.md' '!SECURITY.md' '!.gitignore'

# Remove .git directory as it will be part of the main project
Write-Host "Cleaning up git history..."
Remove-Item -Recurse -Force .git

# Configure build options
Write-Host "Configuring build settings..."
$content = Get-Content CMakeLists.txt
$content = $content -replace 'option\(TIDESDB_WITH_SANITIZER "build with sanitizer in tidesdb" ON\)', 'option(TIDESDB_WITH_SANITIZER "build with sanitizer in tidesdb" OFF)'
$content = $content -replace 'option\(TIDESDB_BUILD_TESTS "enable building tests in tidesdb" ON\)', 'option(TIDESDB_BUILD_TESTS "enable building tests in tidesdb" OFF)'
$content | Set-Content CMakeLists.txt

# Build and install
Write-Host "Building and installing C foundation..."
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build
cmake --install build

Set-Location ..

# Setup Python environment in root directory
Write-Host "Creating Python environment..."
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies and package
Write-Host "Installing dependencies..."
pip install -r requirements.txt
Write-Host "Installing Python package..."
pip install -e .

Write-Host "`nSetup complete! TidesDB Python is ready to use!" -ForegroundColor Green