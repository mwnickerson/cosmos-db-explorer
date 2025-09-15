# Cosmos DB CLI Explorer

A command-line tool for exploring Azure Cosmos DB databases and containers using Account Endpoints and Access Keys.

## Installation

### Prerequisites

- Python 3.11+
- UV package manager ([install UV](https://docs.astral.sh/uv/getting-started/installation/))
- Azure Cosmos DB account with access keys

### Setup

1. Create project directory:
```bash
mkdir cosmos-cli-explorer
cd cosmos-cli-explorer
```

2. Save the code as `main.py` and the `pyproject.toml` file

3. Install dependencies:
```bash
uv sync
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows
```

## Usage

### Basic Commands

```bash
# List all databases
python main.py --endpoint "https://your-account.documents.azure.com:443/" --key "your-key" databases

# List containers in a database
python main.py --endpoint "..." --key "..." containers MyDatabase

# Count documents in a container
python main.py --endpoint "..." --key "..." count MyDatabase MyContainer

# Get recent documents
python main.py --endpoint "..." --key "..." recent MyDatabase MyContainer

# Query documents (default: first 10)
python main.py --endpoint "..." --key "..." query MyDatabase MyContainer

# Get all documents (up to 1000)
python main.py --endpoint "..." --key "..." query MyDatabase MyContainer --all

# Custom query
python main.py --endpoint "..." --key "..." query MyDatabase MyContainer --query "SELECT c.id FROM c"
```

### Interactive Mode

```bash
python main.py --endpoint "..." --key "..." interactive
```

Then use commands like:
- `databases` - List databases
- `containers MyDB` - List containers  
- `count MyDB MyContainer` - Count documents
- `recent MyDB MyContainer` - Get recent documents
- `query MyDB MyContainer` - Query documents
- `help` - Show available commands
- `exit` - Quit

### User Agent Options

```bash
# Use preset user agents for better OPSEC
python main.py --endpoint "..." --key "..." --user-agent dotnet_web databases
python main.py --endpoint "..." --key "..." --user-agent azure_function databases
python main.py --endpoint "..." --key "..." --user-agent power_bi databases
```

Available presets: `dotnet_web`, `node_app`, `python_app`, `azure_function`, `logic_app`, `power_bi`, `azure_databricks`, `postman`, `curl`, `powershell`