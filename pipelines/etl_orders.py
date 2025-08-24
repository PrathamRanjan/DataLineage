import pandas as pd
import time
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from lineage_logger import start_run, log_io, log_column_lineage, end_run

def run_etl_orders():
    start_time = time.time()
    
    # Start lineage tracking
    run_id = start_run("ecommerce", "etl_orders")
    
    try:
        # Read input data
        orders_df = pd.read_csv("data/raw/orders.csv")
        customers_df = pd.read_csv("data/raw/customers.csv")
        
        print(f"Orders columns: {orders_df.columns.tolist()}")
        print(f"Customers columns: {customers_df.columns.tolist()}")
        
        # Log input datasets
        log_io(run_id, 
               inputs=[
                   {
                       "name": "orders.csv",
                       "namespace": "raw_data",
                       "facets": {
                           "schema": [{"name": col, "type": str(orders_df[col].dtype)} for col in orders_df.columns],
                           "rowCount": len(orders_df)
                       }
                   },
                   {
                       "name": "customers.csv", 
                       "namespace": "raw_data",
                       "facets": {
                           "schema": [{"name": col, "type": str(customers_df[col].dtype)} for col in customers_df.columns],
                           "rowCount": len(customers_df)
                       }
                   }
               ],
               row_counts={"orders.csv": len(orders_df), "customers.csv": len(customers_df)}
        )
        
        # Transform: Join orders with customers
        orders_with_customers = orders_df.merge(customers_df, on='customer_id', how='left')
        
        # Transform: Calculate net amount
        orders_with_customers['net_amount'] = orders_with_customers['quantity'] * orders_with_customers['unit_price'] - orders_with_customers['discount']
        
        # Transform: Calculate total order value
        orders_with_customers['total_value'] = orders_with_customers['quantity'] * orders_with_customers['unit_price']
        
        # Transform: Add profit margin (assuming 30% margin)
        orders_with_customers['estimated_profit'] = orders_with_customers['net_amount'] * 0.30
        
        # Transform: Categorize order size
        def categorize_order_size(net_amount):
            if net_amount < 50:
                return 'small'
            elif net_amount < 200:
                return 'medium'
            else:
                return 'large'
        
        orders_with_customers['order_size_category'] = orders_with_customers['net_amount'].apply(categorize_order_size)
        
        # Select final columns (avoiding status column conflict)
        final_columns = [
            'order_id', 'customer_id', 'name', 'email', 'product_name', 
            'category', 'quantity', 'unit_price', 'discount', 'net_amount', 
            'total_value', 'estimated_profit', 'order_size_category', 
            'order_date', 'payment_method', 'segment'
        ]
        
        # Add order status with proper naming
        orders_with_customers['order_status'] = orders_with_customers['status_x']  # from orders
        orders_with_customers['customer_status'] = orders_with_customers['status_y']  # from customers
        final_columns.extend(['order_status', 'customer_status'])
        
        orders_clean = orders_with_customers[final_columns]
        
        # Write output
        output_path = "data/processed/orders_clean.csv"
        orders_clean.to_csv(output_path, index=False)
        
        # Log column lineage
        column_mappings = [
            {
                "downstream": "net_amount",
                "upstream": ["quantity", "unit_price", "discount"],
                "transformation": "quantity * unit_price - discount"
            },
            {
                "downstream": "total_value", 
                "upstream": ["quantity", "unit_price"],
                "transformation": "quantity * unit_price"
            },
            {
                "downstream": "estimated_profit",
                "upstream": ["net_amount"],
                "transformation": "net_amount * 0.30"
            },
            {
                "downstream": "order_size_category",
                "upstream": ["net_amount"],
                "transformation": "categorize_order_size(net_amount)"
            }
        ]
        
        log_column_lineage(run_id, column_mappings)
        
        # Log output dataset
        log_io(run_id,
               outputs=[
                   {
                       "name": "orders_clean.csv",
                       "namespace": "processed_data", 
                       "facets": {
                           "schema": [{"name": col, "type": str(orders_clean[col].dtype)} for col in orders_clean.columns],
                           "rowCount": len(orders_clean)
                       }
                   }
               ],
               row_counts={"orders_clean.csv": len(orders_clean)}
        )
        
        # Calculate duration and end run
        duration_ms = int((time.time() - start_time) * 1000)
        
        end_run(run_id, 
                status="COMPLETED", 
                duration_ms=duration_ms,
                job_namespace="ecommerce",
                job_name="etl_orders",
                input_names="orders.csv,customers.csv",
                output_names="orders_clean.csv", 
                transform="join,calculate_net_amount,categorize_order_size",
                columns_map="net_amount:quantity+unit_price+discount;total_value:quantity+unit_price;estimated_profit:net_amount;order_size_category:net_amount",
                rowcount_in=len(orders_df) + len(customers_df),
                rowcount_out=len(orders_clean)
        )
        
        print(f"ETL Orders pipeline completed successfully. Processed {len(orders_clean)} records.")
        
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        end_run(run_id, 
                status="FAILED", 
                duration_ms=duration_ms,
                job_namespace="ecommerce",
                job_name="etl_orders"
        )
        raise e

if __name__ == "__main__":
    run_etl_orders()