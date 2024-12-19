# Query Processor

A Python script for processing database queries from YAML configuration files. This script supports multiple database adapters (SQLite, MySQL, PostgreSQL) and uses Jinja2 templating for query rendering.

## Features

- Multiple database adapter support (SQLite, MySQL, PostgreSQL)
- Template-based query rendering with Jinja2
- Cascading query execution with nested dictionary results
- Support for initial data injection
- YAML-based configuration
- Proper logging and error handling

## Installation

This script uses UV for dependency management. First, install UV if you haven't already:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```
## Configuration File Structure

The script expects a YAML configuration file with the following structure:

```yaml
adapter_settings:
  local:
    database: db1.db
    adapter: sqlite3
  remote:
    adapter: mysql
    user: root
    password: password
    host: localhost
    port: 3306

queries:
  - table: db1.member
    adapter: local
    query: select * from db1.member where id = {{ member_id }};
  - table: db2.account_information
    adapter: remote
    query: select * from db2.account_information where member_id = {{ db1.member.id }};

output:
  template_context:
    transaction_types:
      1: deposit
      2: withdrawal
  template: |
    {% for transaction in db2.transaction_information %}
    {{ template_context.transaction_types[transaction.transaction_type] }} {{ transaction.amount }}
    {% endfor %}
```

## Usage

Run the script by providing a YAML configuration file and optional initial data:

```bash
uv run cli.py --file=config.yaml --initial_data=member_id=1
```

### Arguments

- `--file`: Path to the YAML configuration file (required)
- `--initial_data`: Initial data in key=value format (optional)
  - Format: key1=value1,key2=value2
  - Example: member_id=1,status=active

### Output

The script will:
1. Process each query in the order defined in the YAML file
2. Build a nested dictionary structure with the results
3. Render the final output using the provided template

## Data Structure

The script builds a nested dictionary structure based on the table names in your queries. For example:

```python
{
    "db1": {
        "member": {
            "id": 1,
            "name": "John Doe"
        }
    },
    "db2": {
        "account_information": {
            "id": 2,
            "member_id": 1
        }
    }
}
```

This structure can be referenced in subsequent queries using Jinja2 template syntax.