# Data Lineage POC

A Python-based proof of concept for tracking data lineage across ETL pipelines. This tool demonstrates how data flows and transforms through multiple processing stages while maintaining comprehensive metadata and visualization capabilities.

## Features

- **Metadata Logging**: Automatic capture of lineage events in JSON and CSV formats
- **Column-Level Lineage**: Track transformations at the field level
- **Visual Diagrams**: Generate Mermaid diagrams showing data flow
- **Privacy Protection**: Hash sensitive data while maintaining analytics capability
- **Comprehensive Documentation**: Detailed glossary of fields and transformations

## Quick Start

### Prerequisites

- Python 3.10+ 
- Virtual environment (recommended)

### Installation

1. Clone or download this repository
2. Set up the environment:

```bash
cd lineage-poc
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate
pip install pandas pyyaml click streamlit networkx plotly
```

### Running the Pipelines

Execute both ETL pipelines to generate sample lineage data:

```bash
# Run orders processing pipeline
python run_pipeline.py --name etl_orders

# Run customer analytics pipeline  
python run_pipeline.py --name etl_customers_daily
```

### Generate Lineage Visualization

**Option 1: Interactive Streamlit Dashboard (Recommended)**

Launch the interactive web-based lineage visualizer:

```bash
streamlit run app/streamlit_app.py
```

This opens a comprehensive dashboard with:
- Interactive network graph of data flows
- Pipeline metrics and success rates
- Column-level lineage details
- Data quality insights
- Raw event exploration

**Option 2: Static Mermaid Diagram**

Create a Mermaid diagram showing data flow:

```bash
python tools/make_mermaid.py lineage/events/events.csv > lineage/diagram.mmd
```

View the diagram in any Mermaid-compatible viewer or VS Code with the Mermaid extension.

## Repository Structure

```
lineage-poc/
├── data/
│   ├── raw/                    # Input CSV files
│   │   ├── customers.csv       # Customer master data
│   │   └── orders.csv          # Order transaction data
│   └── processed/              # ETL outputs
│       ├── orders_clean.csv    # Enriched order data
│       └── customers_daily.csv # Customer analytics
├── lineage/
│   ├── events/                 # JSON lineage events per run
│   └── diagram.mmd             # Generated Mermaid diagram
├── pipelines/
│   ├── etl_orders.py          # Orders processing pipeline
│   └── etl_customers_daily.py # Customer analytics pipeline
├── app/
│   └── streamlit_app.py       # Interactive web dashboard
├── tools/
│   └── make_mermaid.py        # Diagram generator
├── docs/
│   ├── glossary.yaml          # Data dictionary and documentation
│   └── usage.md               # User guide
├── lineage_logger.py          # Core metadata logging
└── run_pipeline.py            # Pipeline execution runner
```

## Pipeline Overview

### ETL Orders Pipeline
- **Purpose**: Enrich order data with customer information and business metrics
- **Inputs**: `customers.csv`, `orders.csv`
- **Outputs**: `orders_clean.csv`
- **Key Transformations**:
  - Customer data join
  - Net amount calculation (quantity × unit_price - discount)
  - Order size categorization (small/medium/large)
  - Profit estimation

### ETL Customers Daily Pipeline  
- **Purpose**: Create privacy-protected customer analytics with behavioral metrics
- **Inputs**: `customers.csv`, `orders.csv`
- **Outputs**: `customers_daily.csv`
- **Key Transformations**:
  - Email hashing for privacy
  - Order aggregation per customer
  - High-value customer identification
  - Customer lifetime value scoring
  - Engagement level categorization

## Lineage Tracking

The system captures comprehensive metadata for each pipeline run:

- **Run-level tracking**: Job execution status, duration, and timestamps
- **Dataset tracking**: Input/output schemas, row counts, and data quality metrics
- **Column-level lineage**: Field-to-field mappings and transformation logic
- **Business context**: Glossary definitions and transformation purposes

### Sample Lineage Events

JSON events are stored in `lineage/events/` with detailed metadata:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2024-03-15T10:30:45",
  "run": {
    "runId": "uuid-string",
    "facets": {
      "status": "COMPLETED",
      "duration_ms": 1250
    }
  }
}
```

CSV summary in `lineage/events/events.csv`:

| event_time | job_name | input_names | output_names | status |
|------------|----------|-------------|--------------|--------|
| 2024-03-15T10:30:45 | etl_orders | customers.csv,orders.csv | orders_clean.csv | COMPLETED |

## Data Dictionary

See `docs/glossary.yaml` for comprehensive documentation including:

- Field definitions and data types
- PII identification and protection methods
- Business rules and transformation logic
- Data quality requirements
- Stakeholder information

## Adding New Pipelines

To instrument a new pipeline with lineage tracking:

1. Import the logger functions:
```python
from lineage_logger import start_run, log_io, log_column_lineage, end_run
```

2. Add the four key instrumentation calls:
```python
# At start
run_id = start_run("namespace", "pipeline_name")

# After reading inputs
log_io(run_id, inputs=[...], row_counts={...})

# After transformations  
log_column_lineage(run_id, mappings=[...])

# At completion
end_run(run_id, status="COMPLETED", duration_ms=duration)
```

3. Add your pipeline to the runner script choices

See `docs/usage.md` for detailed examples and best practices.

## Success Criteria ✅

This POC meets all specified deliverables:

1. ✅ **ETL Pipeline Execution**: Two working pipelines process mock datasets
2. ✅ **Metadata Logging**: JSON and CSV lineage capture implemented  
3. ✅ **Sample Lineage Logs**: Generated from both pipeline runs
4. ✅ **Visual Lineage Diagram**: Mermaid diagram showing data flow
5. ✅ **Glossary Documentation**: Comprehensive data dictionary
6. ✅ **Project Repository**: Clear README and user documentation

## Technical Implementation

- **Language**: Python 3.10+
- **Dependencies**: pandas, pyyaml, click, streamlit, networkx, plotly
- **Data Formats**: CSV (data), JSON (events), YAML (config)
- **Visualization**: Interactive Streamlit dashboard + Mermaid.js diagrams
- **Privacy**: SHA256 hashing for sensitive data protection

## Next Steps

Potential enhancements for production deployment:

- Integration with data catalogs (Apache Atlas, DataHub)
- Real-time lineage streaming to observability platforms
- Advanced visualization with interactive web interfaces
- Integration with workflow orchestrators (Airflow, Prefect)
- Data quality monitoring and alerting
- Role-based access controls for lineage data

## License

This is a proof of concept for educational and demonstration purposes.