# Cosmos DB Explorer

## Installation

### Prerequisites

- Python 3.8+
- UV package manager ([install UV](https://docs.astral.sh/uv/getting-started/installation/))
- Azure Cosmos DB account with access keys

### Setup

1. Clone or create the project:
```bash
mkdir cosmos-cli-explorer
cd cosmos-cli-explorer
```

2. Save the Python code as `cosmos_explorer.py` and the `pyproject.toml` file

3. Install dependencies with UV:
```bash
uv sync
```

4. Activate the virtual environment:
```bash
source .venv/bin/activate  # On Linux/Mac
# or
.venv\Scripts\activate     # On Windows
```

## Usage

### Basic Commands

All commands require your Cosmos DB endpoint and key:

```bash
python cosmos_explorer.py --endpoint "https://your-account.documents.azure.com:443/" --key "your-primary-key" [command]
```

### Available Commands

#### List Databases
```bash
python cosmos_explorer.py --endpoint "..." --key "..." databases
```

#### List Containers
```bash
python cosmos_explorer.py --endpoint "..." --key "..." containers MyDatabase
```

#### Execute Queries
```bash
# Default query (SELECT * FROM c)
python cosmos_explorer.py --endpoint "..." --key "..." query MyDatabase MyContainer

# Custom query
python cosmos_explorer.py --endpoint "..." --key "..." query MyDatabase MyContainer --query "SELECT c.id, c.name FROM c WHERE c.status = 'active'"

# Limit results
python cosmos_explorer.py --endpoint "..." --key "..." query MyDatabase MyContainer --limit 5 --max-items 50
```

#### Get Specific Item
```bash
python cosmos_explorer.py --endpoint "..." --key "..." get MyDatabase MyContainer "item-id" "partition-key-value"
```

#### Interactive Mode
```bash
python cosmos_explorer.py --endpoint "..." --key "..." interactive
```

### Interactive Mode Commands

Once in interactive mode, you can use these commands:

- `databases` - List all databases
- `containers <database_id>` - List containers in a database
- `query <database_id> <container_id> [sql_query]` - Execute a query
- `get <database_id> <container_id> <item_id> <partition_key>` - Get a specific item
- `help` - Show available commands
- `exit` or `quit` - Exit interactive mode

### Environment Variables

For convenience, you can set environment variables:

```bash
export COSMOS_ENDPOINT="https://your-account.documents.azure.com:443/"
export COSMOS_KEY="your-primary-key"
```

Then modify the script to use these as defaults, or create a wrapper script.

## Examples

### Exploring a Database
```bash
# List all databases
python cosmos_explorer.py --endpoint $COSMOS_ENDPOINT --key $COSMOS_KEY databases

# Explore containers in a specific database
python cosmos_explorer.py --endpoint $COSMOS_ENDPOINT --key $COSMOS_KEY containers ProductsDB

# Query all items in a container
python cosmos_explorer.py --endpoint $COSMOS_ENDPOINT --key $COSMOS_KEY query ProductsDB Items

# Query with filtering
python cosmos_explorer.py --endpoint $COSMOS_ENDPOINT --key $COSMOS_KEY query ProductsDB Items \
  --query "SELECT c.id, c.name, c.price FROM c WHERE c.category = 'electronics'"
```

### Interactive Session Example
```bash
$ python cosmos_explorer.py --endpoint $COSMOS_ENDPOINT --key $COSMOS_KEY interactive
✓ Connected to Cosmos DB successfully
Interactive Cosmos DB Explorer
Type 'help' for available commands or 'exit' to quit.

cosmos> databases
┏━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Database ID ┃ Resource ID ┃
┡━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ ProductsDB  │ abc123==    │
│ OrdersDB    │ def456==    │
└─────────────┴─────────────┘

cosmos> containers ProductsDB
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Container ID ┃ Partition Key ┃ Resource ID ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ Items        │ /category     │ ghi789==    │
│ Reviews      │ /productId    │ jkl012==    │
└──────────────┴───────────────┴─────────────┘

cosmos> query ProductsDB Items SELECT c.id, c.name FROM c WHERE c.price > 100
Query: SELECT c.id, c.name FROM c WHERE c.price > 100
Found 3 items (showing first 3)
...
```