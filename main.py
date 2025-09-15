#!/usr/bin/env python3
"""
Cosmos DB CLI Explorer
A command-line tool for exploring Azure Cosmos DB databases and containers.
"""


import json
import sys
from typing import Optional, Dict, Any, List
import click
from azure.cosmos import CosmosClient, exceptions
from rich.console import Console
from rich.table import Table
from rich.json import JSON
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

# Import readline for command history (Unix/Linux/Mac)
try:
    import readline
    READLINE_AVAILABLE = True
except ImportError:
    # On Windows, try pyreadline3 as fallback
    try:
        import pyreadline3 as readline
        READLINE_AVAILABLE = True
    except ImportError:
        READLINE_AVAILABLE = False

console = Console()

# Common User Agent patterns for applications that typically use account keys
COMMON_USER_AGENTS = {
    "dotnet_web": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "node_app": "Mozilla/5.0 (compatible; Node.js)",
    "python_app": "Python-urllib/3.11",
    "azure_function": "Microsoft-Azure-Functions/1.0",
    "logic_app": "Microsoft-Azure-LogicApps/1.0", 
    "power_bi": "Microsoft Power BI",
    "azure_databricks": "Apache-HttpClient/4.5.13 (Java/11.0.16)",
    "postman": "PostmanRuntime/7.32.3",
    "curl": "curl/8.4.0",
    "powershell": "Mozilla/5.0 (Windows NT; Windows NT 10.0; en-US) PowerShell/7.3.8"
}

class CosmosExplorer:
    def __init__(self, endpoint: str, key: str, user_agent: Optional[str] = None):
        """Initialize the Cosmos DB explorer with connection details."""
        self.endpoint = endpoint
        self.key = key
        self.client = None
        self.user_agent = user_agent
        
    def connect(self) -> bool:
        """Establish connection to Cosmos DB."""
        try:
            if self.user_agent:
                # Try to set user agent via user_agent parameter (newer SDK versions)
                try:
                    self.client = CosmosClient(
                        self.endpoint, 
                        self.key,
                        user_agent=self.user_agent
                    )
                except TypeError:
                    # Fallback: if user_agent parameter doesn't exist, use default client
                    # and modify headers at request level (we'll handle this in individual methods)
                    self.client = CosmosClient(self.endpoint, self.key)
            else:
                self.client = CosmosClient(self.endpoint, self.key)
                
            # Test connection by listing databases
            list(self.client.list_databases())
            console.print("[green]✓ Connected to Cosmos DB successfully[/green]")
            return True
        except exceptions.CosmosHttpResponseError as e:
            console.print(f"[red]✗ Connection failed: {e.message}[/red]")
            return False
        except Exception as e:
            console.print(f"[red]✗ Connection failed: {str(e)}[/red]")
            return False
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List all databases in the account."""
        if not self.client:
            console.print("[red]Not connected to Cosmos DB[/red]")
            return []
            
        try:
            databases = []
            for db in self.client.list_databases():
                databases.append({
                    'id': db['id'],
                    'self_link': db.get('_self', ''),
                    'resource_id': db.get('_rid', '')
                })
            return databases
        except exceptions.CosmosHttpResponseError as e:
            console.print(f"[red]Error listing databases: {e.message}[/red]")
            return []
    
    def list_containers(self, database_id: str) -> List[Dict[str, Any]]:
        """List all containers in a database."""
        if not self.client:
            console.print("[red]Not connected to Cosmos DB[/red]")
            return []
            
        try:
            database = self.client.get_database_client(database_id)
            containers = []
            for container in database.list_containers():
                containers.append({
                    'id': container['id'],
                    'partition_key': container.get('partitionKey', {}),
                    'self_link': container.get('_self', ''),
                    'resource_id': container.get('_rid', '')
                })
            return containers
        except exceptions.CosmosHttpResponseError as e:
            console.print(f"[red]Error listing containers: {e.message}[/red]")
            return []
    
    def query_items(self, database_id: str, container_id: str, query: str, 
                   max_items: int = 100) -> List[Dict[str, Any]]:
        """Execute a query against a container."""
        if not self.client:
            console.print("[red]Not connected to Cosmos DB[/red]")
            return []
            
        try:
            database = self.client.get_database_client(database_id)
            container = database.get_container_client(container_id)
            
            items = []
            query_results = container.query_items(
                query=query,
                enable_cross_partition_query=True,
                max_item_count=max_items
            )
            
            # Convert iterator to list
            for item in query_results:
                items.append(item)
                if len(items) >= max_items:  # Safety limit
                    break
                
            return items
        except exceptions.CosmosHttpResponseError as e:
            console.print(f"[red]Error executing query: {e.message}[/red]")
            return []
        except Exception as e:
            console.print(f"[red]Error: {str(e)}[/red]")
            return []
    
    def get_item(self, database_id: str, container_id: str, item_id: str, 
                partition_key: str) -> Optional[Dict[str, Any]]:
        """Get a specific item by ID and partition key."""
        if not self.client:
            console.print("[red]Not connected to Cosmos DB[/red]")
            return None
            
        try:
            database = self.client.get_database_client(database_id)
            container = database.get_container_client(container_id)
            
            item = container.read_item(item=item_id, partition_key=partition_key)
            return item
        except exceptions.CosmosResourceNotFoundError:
            console.print(f"[yellow]Item not found: {item_id}[/yellow]")
            return None
        except exceptions.CosmosHttpResponseError as e:
            console.print(f"[red]Error reading item: {e.message}[/red]")
            return None

def display_databases(databases: List[Dict[str, Any]], explorer: 'CosmosExplorer'):
    """Display databases in a formatted table with container counts."""
    if not databases:
        console.print("[yellow]No databases found[/yellow]")
        return
        
    table = Table(title="Cosmos DB Databases", show_header=True)
    table.add_column("Database ID", style="cyan")
    table.add_column("Containers", style="green", justify="right")
    table.add_column("Resource ID", style="magenta")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Getting container counts...", total=len(databases))
        
        for db in databases:
            try:
                containers = explorer.list_containers(db['id'])
                container_count = len(containers) if containers else 0
                table.add_row(db['id'], str(container_count), db['resource_id'])
            except Exception:
                # If we can't get container count, show "?"
                table.add_row(db['id'], "?", db['resource_id'])
            progress.advance(task)
    
    console.print(table)

def display_containers(containers: List[Dict[str, Any]], database_id: str, explorer: 'CosmosExplorer'):
    """Display containers in a formatted table with document counts."""
    if not containers:
        console.print(f"[yellow]No containers found in database '{database_id}'[/yellow]")
        return
        
    table = Table(title=f"Containers in '{database_id}'", show_header=True)
    table.add_column("Container ID", style="cyan")
    table.add_column("Documents", style="green", justify="right") 
    table.add_column("Partition Key", style="yellow")
    table.add_column("Resource ID", style="magenta")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Counting documents...", total=len(containers))
        
        for container in containers:
            try:
                # Get document count
                count_items = explorer.query_items(database_id, container['id'], "SELECT VALUE COUNT(1) FROM c", 1)
                doc_count = count_items[0] if count_items and len(count_items) > 0 else 0
                doc_count_str = f"{doc_count:,}" if isinstance(doc_count, int) else "?"
            except Exception:
                doc_count_str = "?"
            
            partition_key_paths = container['partition_key'].get('paths', [])
            pk_display = ', '.join(partition_key_paths) if partition_key_paths else 'None'
            
            table.add_row(
                container['id'], 
                doc_count_str,
                pk_display, 
                container['resource_id']
            )
            progress.advance(task)
    
    console.print(table)

def display_items(items: List[Dict[str, Any]], limit: int = 10):
    """Display query results."""
    if not items:
        console.print("[yellow]No items found[/yellow]")
        return
    
    console.print(f"[green]Found {len(items)} items (showing first {min(limit, len(items))})[/green]")
    
    for i, item in enumerate(items[:limit]):
        panel_title = f"Item {i+1} - ID: {item.get('id', 'Unknown')}"
        json_obj = JSON(json.dumps(item, indent=2, default=str))
        console.print(Panel(json_obj, title=panel_title, expand=False))
        console.print()

# CLI Commands
@click.group()
@click.option('--endpoint', required=True, help='Cosmos DB account endpoint URL')
@click.option('--key', required=True, help='Cosmos DB account key')
@click.option('--user-agent', help='Custom User-Agent string or preset (dotnet_web, node_app, python_app, azure_function, logic_app, power_bi, azure_databricks, postman, curl, powershell)')
@click.pass_context
def cli(ctx, endpoint: str, key: str, user_agent: Optional[str]):
    """Cosmos DB CLI Explorer - Explore your Cosmos DB databases and containers."""
    ctx.ensure_object(dict)
    
    # Handle preset user agents or custom ones
    if user_agent:
        if user_agent in COMMON_USER_AGENTS:
            selected_ua = COMMON_USER_AGENTS[user_agent]
            console.print(f"[dim]Using preset User-Agent: {user_agent}[/dim]")
        else:
            selected_ua = user_agent
            console.print(f"[dim]Using custom User-Agent: {user_agent}[/dim]")
    else:
        # Default to a common web browser (most generic)
        selected_ua = COMMON_USER_AGENTS["dotnet_web"]
        console.print("[dim]Using default User-Agent (dotnet_web)[/dim]")
    
    explorer = CosmosExplorer(endpoint, key, selected_ua)
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Connecting to Cosmos DB...", total=None)
        if not explorer.connect():
            sys.exit(1)
        progress.update(task, completed=True)
    
    ctx.obj['explorer'] = explorer

@cli.command()
@click.argument('database_id')
@click.argument('container_id')
@click.option('--limit', '-l', default=10, help='Number of recent documents to retrieve')
@click.option('--timestamp-field', '-t', default='_ts', help='Field to use for sorting by time (default: _ts)')
@click.pass_context
def recent(ctx, database_id: str, container_id: str, limit: int, timestamp_field: str):
    """Get the most recent documents from a container."""
    explorer = ctx.obj['explorer']
    
    # Build query to get most recent documents
    query = f"SELECT * FROM c ORDER BY c.{timestamp_field} DESC"
    
    console.print(f"[blue]Getting {limit} most recent documents from {database_id}/{container_id}[/blue]")
    console.print(f"[dim]Ordering by: {timestamp_field} (descending)[/dim]")
    console.print()
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Fetching recent documents...", total=None)
        items = explorer.query_items(database_id, container_id, query, limit)
        progress.update(task, completed=True)
    
    if items:
        console.print(f"[green]Found {len(items)} recent documents:[/green]")
        display_items(items, limit)
    else:
        console.print("[yellow]No documents found or container is empty[/yellow]")

@cli.command()
@click.argument('database_id')
@click.argument('container_id')
@click.pass_context
def count(ctx, database_id: str, container_id: str):
    """Get the total count of documents in a container."""
    explorer = ctx.obj['explorer']
    
    console.print(f"[blue]Counting documents in {database_id}/{container_id}[/blue]")
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Counting documents...", total=None)
        items = explorer.query_items(database_id, container_id, "SELECT VALUE COUNT(1) FROM c", 1)
        progress.update(task, completed=True)
    
    if items and len(items) > 0:
        document_count = items[0]
        if document_count == 0:
            console.print(f"[yellow]📄 Container '{container_id}' is empty (0 documents)[/yellow]")
        else:
            console.print(f"[green]📄 Container '{container_id}' contains {document_count:,} documents[/green]")
    else:
        console.print("[red]❌ Could not retrieve document count[/red]")

@cli.command()
@click.pass_context
def databases(ctx):
    """List all databases in the account."""
    explorer = ctx.obj['explorer']
    dbs = explorer.list_databases()
    display_databases(dbs, explorer)

@cli.command()
@click.argument('database_id')
@click.pass_context
def containers(ctx, database_id: str):
    """List all containers in a database."""
    explorer = ctx.obj['explorer']
    containers_list = explorer.list_containers(database_id)
    display_containers(containers_list, database_id, explorer)

@cli.command()
@click.argument('database_id')
@click.argument('container_id')
@click.option('--query', '-q', help='Custom SQL query to execute')
@click.option('--limit', '-l', default=10, help='Maximum number of items to display')
@click.option('--max-items', default=10, help='Maximum number of items to fetch from Cosmos DB')
@click.option('--all', '-a', is_flag=True, help='Fetch all documents (overrides --max-items with 1000)')
@click.pass_context
def query(ctx, database_id: str, container_id: str, query: str, limit: int, max_items: int, all: bool):
    """Execute a SQL query against a container. Default: get first 10 documents."""
    explorer = ctx.obj['explorer']
    
    # Handle default query and --all flag
    if not query:
        query = 'SELECT * FROM c'
    
    if all:
        max_items = 1000  # Reasonable upper limit to prevent accidents
        console.print("[yellow]⚠️  Fetching up to 1000 documents[/yellow]")
    
    console.print(f"[blue]Executing query on {database_id}/{container_id}:[/blue]")
    console.print(f"[dim]{query}[/dim]")
    console.print(f"[dim]Fetching up to {max_items} documents, displaying {limit}[/dim]")
    console.print()
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Executing query...", total=None)
        items = explorer.query_items(database_id, container_id, query, max_items)
        progress.update(task, completed=True)
    
    display_items(items, limit)

@cli.command()
@click.argument('database_id')
@click.argument('container_id')
@click.argument('item_id')
@click.argument('partition_key')
@click.pass_context
def get(ctx, database_id: str, container_id: str, item_id: str, partition_key: str):
    """Get a specific item by ID and partition key."""
    explorer = ctx.obj['explorer']
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Fetching item...", total=None)
        item = explorer.get_item(database_id, container_id, item_id, partition_key)
        progress.update(task, completed=True)
    
    if item:
        json_obj = JSON(json.dumps(item, indent=2, default=str))
        console.print(Panel(json_obj, title=f"Item: {item_id}", expand=False))

@cli.command()
@click.pass_context
def interactive(ctx):
    """Start interactive mode for exploring Cosmos DB."""
    explorer = ctx.obj['explorer']
    
    console.print("[green]Interactive Cosmos DB Explorer[/green]")
    console.print("Type 'help' for available commands or 'exit' to quit.")
    
    if READLINE_AVAILABLE:
        console.print("[dim]📝 Command history enabled - use ↑/↓ arrows to navigate previous commands[/dim]")
        # Enable command history and auto-completion
        readline.parse_and_bind('tab: complete')
        # Set up command history
        readline.set_history_length(100)
    else:
        console.print("[dim]💡 Install 'pyreadline3' for command history support on Windows[/dim]")
    
    console.print()
    
    while True:
        try:
            if READLINE_AVAILABLE:
                # Use readline for better input handling
                command = input("cosmos> ").strip()
            else:
                # Fallback to basic input
                command = input("cosmos> ").strip()
            
            # Skip empty commands
            if not command:
                continue
                
            if command.lower() in ['exit', 'quit', 'q']:
                console.print("Goodbye! 👋")
                break
            elif command.lower() == 'help':
                console.print("""
Available commands:
  databases                           - List all databases
  containers <database_id>            - List containers in database
  count <database_id> <container_id>  - Count documents in container
  recent <database_id> <container_id> - Get 10 most recent documents
  query <db> <container> [sql]        - Execute query (default: first 10 documents)
  query <db> <container> --all        - Get up to 1000 documents
  get <db> <container> <id> <pk>      - Get specific item
  history                            - Show command history
  clear                              - Clear screen
  help                               - Show this help
  exit/quit/q                        - Exit interactive mode
                """)
            elif command.lower() == 'history':
                if READLINE_AVAILABLE:
                    console.print("[blue]Command History:[/blue]")
                    for i in range(1, readline.get_current_history_length() + 1):
                        hist_item = readline.get_history_item(i)
                        if hist_item:
                            console.print(f"  {i}: {hist_item}")
                else:
                    console.print("[yellow]Command history not available (readline not installed)[/yellow]")
            elif command.lower() == 'clear':
                import os
                os.system('cls' if os.name == 'nt' else 'clear')
                console.print("[green]Interactive Cosmos DB Explorer[/green]")
                console.print("Type 'help' for available commands or 'exit' to quit.")
                console.print()
            elif command.lower() == 'databases':
                dbs = explorer.list_databases()
                display_databases(dbs, explorer)
            elif command.startswith('recent '):
            elif command.startswith('recent '):
            elif command.lower() == 'databases':
                dbs = explorer.list_databases()
                display_databases(dbs)
            elif command.startswith('containers '):
                parts = command.split(' ', 1)
                if len(parts) == 2:
                    db_id = parts[1].strip()
                    containers_list = explorer.list_containers(db_id)
                    display_containers(containers_list, db_id, explorer)
                else:
                    console.print("[red]Usage: containers <database_id>[/red]")
            elif command.startswith('recent '):
                parts = command.split(' ')
                if len(parts) >= 3:
                    db_id = parts[1]
                    container_id = parts[2]
                    limit = 10
                    
                    # Check if user specified a limit
                    if len(parts) > 3 and parts[3].isdigit():
                        limit = int(parts[3])
                    
                    query = "SELECT * FROM c ORDER BY c._ts DESC"
                    console.print(f"[blue]Getting {limit} most recent documents from {db_id}/{container_id}[/blue]")
                    console.print(f"[dim]Ordering by: _ts (descending)[/dim]")
                    
                    items = explorer.query_items(db_id, container_id, query, limit)
                    if items:
                        console.print(f"[green]Found {len(items)} recent documents:[/green]")
                        display_items(items, limit)
                    else:
                        console.print("[yellow]No documents found or container is empty[/yellow]")
                else:
                    console.print("[red]Usage: recent <database_id> <container_id> [limit][/red]")
                    console.print("[dim]Example: recent MyDB MyContainer 5[/dim]")
            elif command.startswith('count '):
                parts = command.split(' ')
                if len(parts) == 3:
                    db_id = parts[1]
                    container_id = parts[2]
                    
                    console.print(f"[blue]Counting documents in {db_id}/{container_id}[/blue]")
                    items = explorer.query_items(db_id, container_id, "SELECT VALUE COUNT(1) FROM c", 1)
                    
                    if items and len(items) > 0:
                        document_count = items[0]
                        if document_count == 0:
                            console.print(f"[yellow]📄 Container '{container_id}' is empty (0 documents)[/yellow]")
                        else:
                            console.print(f"[green]📄 Container '{container_id}' contains {document_count:,} documents[/green]")
                    else:
                        console.print("[red]❌ Could not retrieve document count[/red]")
                else:
                    console.print("[red]Usage: count <database_id> <container_id>[/red]")
            elif command.startswith('query '):
                parts = command.split(' ')
                if len(parts) >= 3:
                    db_id = parts[1]
                    container_id = parts[2]
                    
                    # Check for --all flag
                    if '--all' in parts:
                        max_items = 1000
                        # Remove --all from parts for query parsing
                        parts = [p for p in parts if p != '--all']
                        console.print("[yellow]⚠️  Fetching up to 1000 documents[/yellow]")
                    else:
                        max_items = 10
                    
                    # Get custom query if provided (after db and container)
                    if len(parts) > 3:
                        query_sql = ' '.join(parts[3:])
                    else:
                        query_sql = 'SELECT * FROM c'
                    
                    console.print(f"[blue]Query: {query_sql}[/blue]")
                    console.print(f"[dim]Fetching up to {max_items} documents[/dim]")
                    items = explorer.query_items(db_id, container_id, query_sql, max_items)
                    display_items(items, 10)  # Always display max 10 in interactive mode
                else:
                    console.print("[red]Usage: query <database_id> <container_id> [sql_query] [--all][/red]")
                    console.print("[dim]Examples:[/dim]")
                    console.print("[dim]  query MyDB MyContainer[/dim]")
                    console.print("[dim]  query MyDB MyContainer --all[/dim]") 
                    console.print("[dim]  query MyDB MyContainer SELECT c.id FROM c[/dim]")
            elif command.startswith('get '):
                parts = command.split(' ')
                if len(parts) == 5:
                    _, db_id, container_id, item_id, pk = parts
                    item = explorer.get_item(db_id, container_id, item_id, pk)
                    if item:
                        json_obj = JSON(json.dumps(item, indent=2, default=str))
                        console.print(Panel(json_obj, title=f"Item: {item_id}", expand=False))
                else:
                    console.print("[red]Usage: get <database_id> <container_id> <item_id> <partition_key>[/red]")
            elif command:
                console.print(f"[red]Unknown command: {command}[/red]")
                console.print("Type 'help' for available commands.")
            
        except KeyboardInterrupt:
            console.print("\nGoodbye! 👋")
            break
        except EOFError:
            console.print("Goodbye! 👋")
            break

if __name__ == '__main__':
    cli()