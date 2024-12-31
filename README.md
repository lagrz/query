# Query Processor

A Python script for processing database queries from YAML configuration files. This script supports multiple database adapters (SQLite, MySQL, HTTP) and uses Jinja2 templating for query rendering. Version 0.1.0.

## Features

- Multiple database adapter support (SQLite, MySQL, HTTP)
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
  api:
    adapter: http
    base_url: https://api.example.com
    base_path: /v1
    base_payload:
      format: json
    base_headers:
      Authorization: "Bearer {{ env.API_TOKEN }}"
    http_method: post  # Optional, defaults to post

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
uv run query --file=config.yaml --initial_data=member_id=1
```

Alternatively you can install it using pipx first:

```bash
pipx install .
query --file=config.yaml --initial_data=member_id=1
```

### Arguments

- `--file`: Path to the YAML configuration file (required)
- `--initial_data`: Initial data in key=value format (optional)
  - Format: key1=value1,key2=value2
  - Example: member_id=1,status=active
- `--output`: Path to output file (optional)
- `--log-level`: Set the logging level (optional)
  - Choices: DEBUG, INFO, WARNING, ERROR, CRITICAL
  - Default: INFO

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

## Custom Template Filters

The script includes custom Jinja2 filters to enhance template functionality:

### find_by_key_value

Finds items in a list or dictionary by key and value:

```jinja
{{ collection | find_by_key_value(key, value, default=None) }}
```
 - collection: A list or dictionary to search
 - key: The key to match
 - value: The value to match
 - default: A fallback value if nothing matches (optional)

Finding a specific transaction by ID: 
```jinja
{% set transaction = transactions | find_by_key_value('id', '12345') %}
{{ transaction.amount if transaction else 'Transaction not found' }}
```

Finding in nested data
```jinja
{% set user_info = users | find_by_key_value('email', current_email) %}
{% if user_info %}
  Name: {{ user_info.name }}
  Role: {{ user_info.role }}
{% endif %}
```

Parameters:
- `haystack`: List or dictionary to search
- `key`: Key to match
- `value`: Value to match
- `default`: Default value if no match found (optional)

## Environment Variable Support

The adapter settings support environment variable interpolation using Jinja2 syntax:

```yaml
adapter_settings:
  api:
    base_headers:
      Authorization: "Bearer {{ env.API_TOKEN }}"
```

## HTTP Adapter

The HTTP adapter allows querying REST APIs. Queries should be JSON strings containing:
- `endpoint`: API endpoint to call
- `payload`: Additional payload to merge with base_payload

Example query:
```yaml
queries:
  - table: api.users
    adapter: api
    query: '{"endpoint": "users/search", "payload": {"status": "active"}}'
```

The adapter supports both JSON and CSV responses. Set `csv: 1` in the payload to parse CSV responses.