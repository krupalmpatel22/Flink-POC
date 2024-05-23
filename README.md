# Project Setup Guide

## Requirements

To set up and run this project, ensure you have the following installed on your system:

- **Python 3.10**
- **Java JDK 11**

## Installation Instructions

Follow these steps to install the necessary software and dependencies.

### 1. Install Python 3.10

First, check if Python 3.10 is already installed:

```sh
python3.10 --version

sudo apt update
sudo apt install python3.10
sudo apt install python3.10-venv python3.10-dev

java -version

sudo apt update
sudo apt install openjdk-11-jdk

python3.10 -m venv venv
source venv/bin/activate

pip install -r requirements.txt

source venv/bin/activate

python your_script.py
```

### Explanation of Changes:

1. **Included the `pip install -r requirements.txt` Step**: This crucial step has been added to ensure all required Python packages are installed from the `requirements.txt` file.
2. **Maintained Clear Structure**: The instructions remain clear and organized for ease of use.

This update provides comprehensive guidance for setting up and running the project, including installing dependencies from `requirements.txt`.
