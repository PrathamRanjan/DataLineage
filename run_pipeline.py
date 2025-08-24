#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path

# Add the pipelines directory to the path
pipelines_dir = Path(__file__).parent / "pipelines"
sys.path.append(str(pipelines_dir))

def main():
    parser = argparse.ArgumentParser(description="Run ETL pipelines with data lineage tracking")
    parser.add_argument("--name", required=True, 
                       choices=["etl_orders", "etl_customers_daily"],
                       help="Name of the pipeline to run")
    
    args = parser.parse_args()
    
    try:
        if args.name == "etl_orders":
            from etl_orders import run_etl_orders
            print("Starting ETL Orders pipeline...")
            run_etl_orders()
            
        elif args.name == "etl_customers_daily":
            from etl_customers_daily import run_etl_customers_daily
            print("Starting ETL Customers Daily pipeline...")
            run_etl_customers_daily()
            
        print(f"Pipeline '{args.name}' completed successfully!")
        
    except Exception as e:
        print(f"Pipeline '{args.name}' failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()