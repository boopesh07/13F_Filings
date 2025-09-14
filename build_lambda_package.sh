#!/bin/bash
# Exit immediately if a command exits with a non-zero status.
set -e

# Define variables for clarity
PACKAGE_DIR="lambda_package"
ZIP_FILE="13f_filings_lambda.zip"

# 1. Clean up previous builds to ensure a fresh package
echo "Cleaning up previous build artifacts..."
rm -rf "$PACKAGE_DIR"
rm -f "$ZIP_FILE"

# 2. Create the directory to stage the Lambda package
echo "Creating staging directory: $PACKAGE_DIR"
mkdir "$PACKAGE_DIR"

# 3. Install production dependencies into the staging directory
echo "Installing dependencies from requirements.txt..."
pip install --platform manylinux2014_x86_64 --implementation cp --python-version 3.13 --only-binary=:all: -r requirements.txt -t "$PACKAGE_DIR"

# 4. Copy the main application script into the package
echo "Copying application script..."
cp 13f_filings.py "$PACKAGE_DIR"

# 5. Create the final ZIP archive from the staging directory
echo "Creating Lambda deployment package: $ZIP_FILE..."
(cd "$PACKAGE_DIR" && zip -r "../$ZIP_FILE" .)

echo "Successfully created Lambda deployment package: $ZIP_FILE"
