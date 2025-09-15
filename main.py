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

console = Console()

class CosmosExplorer:
    def __init__(self, endpoint: str, key: str):
        """Initialize the Cosmos DB explorer with connection details."""
        self.endpoint = endpoint
        self.key = key
        self.client = None
        
    def connect(self) -> bool:
        """Establish connection to Cosmos DB."""
        try:
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

def display_databases(databases: List[Dict[str, Any]]):
    """Display databases in a formatted table."""
    if not databases:
        console.print("[yellow]No databases found[/yellow]")
        return
        
    table = Table(title="Cosmos DB Databases", show_header=True)
    table.add_column("Database ID", style="cyan")
    table.add_column("Resource ID", style="green")
    
    for db in databases:
        table.add_row(db['id'], db['resource_id'])
    
    console.print(table)

def display_containers(containers: List[Dict[str, Any]], database_id: str):
    """Display containers in a formatted table."""
    if not containers:
        console.print(f"[yellow]No containers found in database '{database_id}'[/yellow]")
        return
        
    table = Table(title=f"Containers in '{database_id}'", show_header=True)
    table.add_column("Container ID", style="cyan")
    table.add_column("Partition Key", style="green")
    table.add_column("Resource ID", style="magenta")
    
    for container in containers:
        partition_key_paths = container['partition_key'].get('paths', [])
        pk_display = ', '.join(partition_key_paths) if partition_key_paths else 'None'
        table.add_row(container['id'], pk_display, container['resource_id'])
    
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
@click.pass_context
def cli(ctx, endpoint: str, key: str):
    """Cosmos DB CLI Explorer - Explore your Cosmos DB databases and containers."""
    ctx.ensure_object(dict)
    explorer = CosmosExplorer(endpoint, key)
    
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
@click.pass_context
def databases(ctx):
    """List all databases in the account."""
    explorer = ctx.obj['explorer']
    dbs = explorer.list_databases()
    display_databases(dbs)

@cli.command()
@click.argument('database_id')
@click.pass_context
def containers(ctx, database_id: str):
    """List all containers in a database."""
    explorer = ctx.obj['explorer']
    containers_list = explorer.list_containers(database_id)
    display_containers(containers_list, database_id)

@cli.command()
@click.argument('database_id')
@click.argument('container_id')
@click.option('--query', '-q', default='SELECT * FROM c', help='SQL query to execute')
@click.option('--limit', '-l', default=10, help='Maximum number of items to display')
@click.option('--max-items', default=100, help='Maximum number of items to fetch')
@click.pass_context
def query(ctx, database_id: str, container_id: str, query: str, limit: int, max_items: int):
    """Execute a SQL query against a container."""
    explorer = ctx.obj['explorer']
    
    console.print(f"[blue]Executing query on {database_id}/{container_id}:[/blue]")
    console.print(f"[dim]{query}[/dim]")
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
    console.print()
    
    while True:
        try:
            command = input("cosmos> ").strip()
            
            if command.lower() in ['exit', 'quit', 'q']:
                console.print("Goodbye! 👋")
                break
            elif command.lower() == 'help':
                console.print("""
Available commands:
  databases                           - List all databases
  containers <database_id>            - List containers in database
  query <db> <container> [sql]        - Execute query (default: SELECT * FROM c)
  get <db> <container> <id> <pk>      - Get specific item
  help                               - Show this help
  exit/quit/q                        - Exit interactive mode
                """)
            elif command.lower() == 'databases':
                dbs = explorer.list_databases()
                display_databases(dbs)
            elif command.startswith('containers '):
                parts = command.split(' ', 1)
                if len(parts) == 2:
                    db_id = parts[1].strip()
                    containers_list = explorer.list_containers(db_id)
                    display_containers(containers_list, db_id)
                else:
                    console.print("[red]Usage: containers <database_id>[/red]")
            elif command.startswith('query '):
                parts = command.split(' ', 3)
                if len(parts) >= 3:
                    db_id = parts[1]
                    container_id = parts[2]
                    query_sql = parts[3] if len(parts) > 3 else 'SELECT * FROM c'
                    
                    console.print(f"[blue]Query: {query_sql}[/blue]")
                    items = explorer.query_items(db_id, container_id, query_sql)
                    display_items(items)
                else:
                    console.print("[red]Usage: query <database_id> <container_id> [sql_query][/red]")
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