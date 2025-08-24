# User Guide - Data Lineage POC

This guide provides detailed instructions for using and extending the data lineage tracking system.

## Getting Started

### Environment Setup

1. **Create Virtual Environment**:
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
```

2. **Install Dependencies**:
```bash
pip install pandas pyyaml click
# Optional for UI development
pip install streamlit networkx pyvis
```

3. **Verify Installation**:
```bash
python -c "import pandas, yaml, click; print('Dependencies installed successfully')"
```

### Running Your First Pipeline

1. **Execute Orders Pipeline**:
```bash
python run_pipeline.py --name etl_orders
```

Expected output:
```
Starting ETL Orders pipeline...
Orders columns: ['order_id', 'customer_id', 'product_name', ...]
Customers columns: ['customer_id', 'name', 'email', ...]
ETL Orders pipeline completed successfully. Processed 25 records.
Pipeline 'etl_orders' completed successfully!
```

2. **Execute Customer Analytics Pipeline**:
```bash
python run_pipeline.py --name etl_customers_daily
```

3. **Generate Lineage Diagram**:
```bash
python tools/make_mermaid.py lineage/events/events.csv > lineage/diagram.mmd
```

## Understanding the Output

### Processed Data Files

After running both pipelines, check the outputs:

```bash
ls -la data/processed/
# orders_clean.csv - Enriched order data with customer info
# customers_daily.csv - Customer analytics with privacy protection
```

### Lineage Events

JSON events are stored per pipeline run:

```bash
ls -la lineage/events/
# <run-id>.json - Initial run event
# <run-id>_io.json - Input/output logging
# <run-id>_lineage.json - Column lineage mappings
# <run-id>_complete.json - Completion event
# events.csv - Aggregated summary
```

### Sample JSON Event Structure

**Start Event** (`<run-id>.json`):
```json
{
  "eventType": "START",
  "eventTime": "2024-03-15T10:30:00",
  "run": {
    "runId": "abc123-def456",
    "facets": {}
  },
  "job": {
    "namespace": "ecommerce",
    "name": "etl_orders",
    "facets": {}
  }
}
```

**Column Lineage Event** (`<run-id>_lineage.json`):
```json
{
  "eventType": "COLUMN_LINEAGE",
  "eventTime": "2024-03-15T10:30:30",
  "run": {
    "runId": "abc123-def456"
  },
  "columnLineage": {
    "fields": [
      {
        "downstream": "net_amount",
        "upstream": ["quantity", "unit_price", "discount"],
        "transformation": "quantity * unit_price - discount"
      }
    ]
  }
}
```

## Adding Custom Pipelines

### Template Pipeline Structure

Create a new pipeline using this template:

```python
import pandas as pd
import time
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from lineage_logger import start_run, log_io, log_column_lineage, end_run

def run_my_pipeline():
    start_time = time.time()
    
    # 1. Start lineage tracking
    run_id = start_run("my_namespace", "my_pipeline_name")
    
    try:
        # 2. Read input data
        input_df = pd.read_csv("data/raw/my_input.csv")
        
        # 3. Log input datasets
        log_io(run_id, 
               inputs=[{
                   "name": "my_input.csv",
                   "namespace": "raw_data",
                   "facets": {
                       "schema": [{"name": col, "type": str(input_df[col].dtype)} 
                                 for col in input_df.columns],
                       "rowCount": len(input_df)
                   }
               }],
               row_counts={"my_input.csv": len(input_df)}
        )
        
        # 4. Perform transformations
        output_df = input_df.copy()
        output_df['new_field'] = output_df['existing_field'] * 2
        
        # 5. Write output
        output_df.to_csv("data/processed/my_output.csv", index=False)
        
        # 6. Log column lineage for new fields
        column_mappings = [{
            "downstream": "new_field",
            "upstream": ["existing_field"], 
            "transformation": "existing_field * 2"
        }]
        log_column_lineage(run_id, column_mappings)
        
        # 7. Log output dataset
        log_io(run_id,
               outputs=[{
                   "name": "my_output.csv",
                   "namespace": "processed_data",
                   "facets": {
                       "schema": [{"name": col, "type": str(output_df[col].dtype)} 
                                 for col in output_df.columns],
                       "rowCount": len(output_df)
                   }
               }],
               row_counts={"my_output.csv": len(output_df)}
        )
        
        # 8. End run successfully
        duration_ms = int((time.time() - start_time) * 1000)
        end_run(run_id, 
                status="COMPLETED",
                duration_ms=duration_ms,
                job_namespace="my_namespace",
                job_name="my_pipeline_name",
                input_names="my_input.csv",
                output_names="my_output.csv",
                transform="multiply_by_two",
                columns_map="new_field:existing_field",
                rowcount_in=len(input_df),
                rowcount_out=len(output_df)
        )
        
        print(f"Pipeline completed successfully. Processed {len(output_df)} records.")
        
    except Exception as e:
        # Handle failures
        duration_ms = int((time.time() - start_time) * 1000)
        end_run(run_id, 
                status="FAILED",
                duration_ms=duration_ms,
                job_namespace="my_namespace", 
                job_name="my_pipeline_name"
        )
        raise e

if __name__ == "__main__":
    run_my_pipeline()
```

### Adding to Runner Script

Update `run_pipeline.py` to include your new pipeline:

```python
parser.add_argument("--name", required=True, 
                   choices=["etl_orders", "etl_customers_daily", "my_pipeline"],
                   help="Name of the pipeline to run")

# Add in the main function:
elif args.name == "my_pipeline":
    from my_pipeline import run_my_pipeline
    print("Starting My Pipeline...")
    run_my_pipeline()
```

## Lineage Logger API Reference

### Core Functions

#### `start_run(job_namespace: str, job_name: str) -> str`
- **Purpose**: Initialize a new pipeline run
- **Parameters**:
  - `job_namespace`: Logical grouping (e.g., "ecommerce", "finance")
  - `job_name`: Specific pipeline identifier
- **Returns**: Unique run ID for subsequent logging calls

#### `log_io(run_id: str, inputs: List[Dict], outputs: List[Dict], ...)`
- **Purpose**: Log input/output datasets and schemas
- **Parameters**:
  - `run_id`: From start_run()
  - `inputs`: List of input dataset metadata
  - `outputs`: List of output dataset metadata
  - `row_counts`: Dictionary of dataset names to row counts

#### `log_column_lineage(run_id: str, mappings: List[Dict])`
- **Purpose**: Record field-level transformations
- **Parameters**:
  - `run_id`: From start_run()
  - `mappings`: List of column transformation definitions

#### `end_run(run_id: str, status: str, duration_ms: int, ...)`
- **Purpose**: Complete pipeline run logging
- **Parameters**:
  - `run_id`: From start_run()
  - `status`: "COMPLETED" or "FAILED"
  - `duration_ms`: Execution time in milliseconds
  - Additional metadata for CSV logging

### Column Lineage Mapping Format

```python
{
    "downstream": "target_field_name",
    "upstream": ["source_field1", "source_field2"],
    "transformation": "human_readable_description"
}
```

Examples:
```python
# Simple calculation
{
    "downstream": "total_amount",
    "upstream": ["quantity", "unit_price"],
    "transformation": "quantity * unit_price"
}

# Complex business logic
{
    "downstream": "customer_segment",
    "upstream": ["total_spent", "order_count", "signup_date"],
    "transformation": "segment_classification(total_spent, order_count, days_since_signup)"
}

# Privacy transformation
{
    "downstream": "email_hash",
    "upstream": ["email"],
    "transformation": "sha256_hash(email)[:16]"
}
```

## Visualization Options

### Mermaid Diagrams

Generate static diagrams:
```bash
python tools/make_mermaid.py lineage/events/events.csv > lineage/diagram.mmd
```

View in:
- VS Code with Mermaid extension
- GitHub (renders Mermaid automatically)
- Online Mermaid editors
- Documentation sites

### Custom Visualization

For advanced visualization needs, create custom tools:

```python
import pandas as pd
import networkx as nx

def create_lineage_graph():
    # Read events
    events_df = pd.read_csv("lineage/events/events.csv")
    
    # Build graph
    G = nx.DiGraph()
    
    for _, row in events_df.iterrows():
        job = row['job_name']
        inputs = row['input_names'].split(',') if row['input_names'] else []
        outputs = row['output_names'].split(',') if row['output_names'] else []
        
        # Add nodes and edges
        for inp in inputs:
            if inp.strip():
                G.add_edge(inp.strip(), job)
        for out in outputs:
            if out.strip():
                G.add_edge(job, out.strip())
    
    return G
```

## Troubleshooting

### Common Issues

**1. Module Import Errors**
```bash
# Ensure you're in the project root
cd lineage-poc
python run_pipeline.py --name etl_orders
```

**2. Pandas/NumPy Compatibility**
```bash
pip install "numpy<2"  # If encountering version conflicts
```

**3. Missing Output Directories**
```bash
mkdir -p data/processed lineage/events
```

**4. Column Name Conflicts**
- Use suffixes in pandas merges: `pd.merge(df1, df2, suffixes=('_left', '_right'))`
- Reference specific columns: `df['column_x']` vs `df['column_y']`

### Debugging Pipeline Execution

Add debug output to your pipelines:

```python
# After reading data
print(f"Input shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")

# After transformations
print(f"Output shape: {result_df.shape}")
print(f"Sample data:\\n{result_df.head()}")
```

### Validating Lineage Events

Check event generation:

```bash
# List generated events
ls -la lineage/events/

# View CSV summary
cat lineage/events/events.csv

# Pretty-print JSON event
python -m json.tool lineage/events/<run-id>.json
```

## Best Practices

### Pipeline Design

1. **Atomic Operations**: Keep transformations focused and single-purpose
2. **Error Handling**: Always wrap pipeline logic in try/catch
3. **Idempotency**: Ensure pipelines can be safely re-run
4. **Schema Validation**: Verify input data structure before processing

### Lineage Tracking

1. **Meaningful Names**: Use descriptive job namespaces and names
2. **Comprehensive Mapping**: Log all derived fields with lineage
3. **Business Context**: Include transformation rationale in descriptions
4. **Consistent Timing**: Use millisecond precision for duration tracking

### Data Management

1. **Version Control**: Track changes to pipeline logic
2. **Data Validation**: Implement quality checks before and after processing
3. **Privacy Protection**: Hash or encrypt sensitive fields
4. **Retention Policies**: Define data lifecycle management

This completes the user guide for the Data Lineage POC. Use this documentation to understand the system and extend it for your specific use cases.