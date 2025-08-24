import pandas as pd
import time
import hashlib
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).parent.parent))

from lineage_logger import start_run, log_io, log_column_lineage, end_run

def run_etl_customers_daily():
    start_time = time.time()
    
    # Start lineage tracking
    run_id = start_run("ecommerce", "etl_customers_daily")
    
    try:
        # Read input data
        customers_df = pd.read_csv("data/raw/customers.csv")
        orders_df = pd.read_csv("data/raw/orders.csv")
        
        # Log input datasets
        log_io(run_id,
               inputs=[
                   {
                       "name": "customers.csv",
                       "namespace": "raw_data",
                       "facets": {
                           "schema": [{"name": col, "type": str(customers_df[col].dtype)} for col in customers_df.columns],
                           "rowCount": len(customers_df)
                       }
                   },
                   {
                       "name": "orders.csv",
                       "namespace": "raw_data", 
                       "facets": {
                           "schema": [{"name": col, "type": str(orders_df[col].dtype)} for col in orders_df.columns],
                           "rowCount": len(orders_df)
                       }
                   }
               ],
               row_counts={"customers.csv": len(customers_df), "orders.csv": len(orders_df)}
        )
        
        # Transform: Filter active customers only
        active_customers = customers_df[customers_df['status'] == 'active'].copy()
        
        # Transform: Hash email addresses for privacy
        def hash_email(email):
            return hashlib.sha256(email.encode()).hexdigest()[:16]
        
        active_customers['email_hash'] = active_customers['email'].apply(hash_email)
        
        # Transform: Calculate customer value metrics
        # Aggregate order data per customer
        customer_orders = orders_df[orders_df['status'] == 'completed'].groupby('customer_id').agg({
            'order_id': 'count',
            'quantity': 'sum', 
            'unit_price': 'mean',
            'discount': 'sum'
        }).reset_index()
        
        customer_orders.columns = ['customer_id', 'total_orders', 'total_quantity', 'avg_unit_price', 'total_discount']
        
        # Calculate total spent
        orders_value = orders_df[orders_df['status'] == 'completed'].copy()
        orders_value['net_amount'] = orders_value['quantity'] * orders_value['unit_price'] - orders_value['discount']
        customer_spending = orders_value.groupby('customer_id')['net_amount'].sum().reset_index()
        customer_spending.columns = ['customer_id', 'total_spent']
        
        # Join customer data with order metrics
        customers_enriched = active_customers.merge(customer_orders, on='customer_id', how='left')
        customers_enriched = customers_enriched.merge(customer_spending, on='customer_id', how='left')
        
        # Fill NaN values for customers without orders
        customers_enriched = customers_enriched.fillna({
            'total_orders': 0,
            'total_quantity': 0,
            'avg_unit_price': 0,
            'total_discount': 0,
            'total_spent': 0
        })
        
        # Transform: Determine high-value customers
        def is_high_value(total_spent, total_orders):
            return (total_spent > 500) or (total_orders > 5)
        
        customers_enriched['is_high_value'] = customers_enriched.apply(
            lambda row: is_high_value(row['total_spent'], row['total_orders']), axis=1
        )
        
        # Transform: Calculate customer lifetime value score
        customers_enriched['clv_score'] = (
            customers_enriched['total_spent'] * 0.6 + 
            customers_enriched['total_orders'] * 50 + 
            customers_enriched['total_quantity'] * 10
        )
        
        # Transform: Categorize customers by engagement
        def categorize_engagement(total_orders):
            if total_orders == 0:
                return 'inactive'
            elif total_orders <= 2:
                return 'low'
            elif total_orders <= 5:
                return 'medium'
            else:
                return 'high'
        
        customers_enriched['engagement_level'] = customers_enriched['total_orders'].apply(categorize_engagement)
        
        # Select final columns
        final_columns = [
            'customer_id', 'name', 'email_hash', 'city', 'state', 'country',
            'signup_date', 'segment', 'total_orders', 'total_quantity',
            'avg_unit_price', 'total_discount', 'total_spent', 'is_high_value',
            'clv_score', 'engagement_level'
        ]
        
        customers_daily = customers_enriched[final_columns]
        
        # Remove duplicates (deduplication)
        customers_daily = customers_daily.drop_duplicates(subset=['customer_id'])
        
        # Write output
        output_path = "data/processed/customers_daily.csv"
        customers_daily.to_csv(output_path, index=False)
        
        # Log column lineage
        column_mappings = [
            {
                "downstream": "email_hash",
                "upstream": ["email"],
                "transformation": "sha256_hash(email)[:16]"
            },
            {
                "downstream": "is_high_value",
                "upstream": ["total_spent", "total_orders"],
                "transformation": "(total_spent > 500) OR (total_orders > 5)"
            },
            {
                "downstream": "clv_score",
                "upstream": ["total_spent", "total_orders", "total_quantity"],
                "transformation": "total_spent * 0.6 + total_orders * 50 + total_quantity * 10"
            },
            {
                "downstream": "engagement_level",
                "upstream": ["total_orders"],
                "transformation": "categorize_engagement(total_orders)"
            }
        ]
        
        log_column_lineage(run_id, column_mappings)
        
        # Log output dataset
        log_io(run_id,
               outputs=[
                   {
                       "name": "customers_daily.csv",
                       "namespace": "processed_data",
                       "facets": {
                           "schema": [{"name": col, "type": str(customers_daily[col].dtype)} for col in customers_daily.columns],
                           "rowCount": len(customers_daily)
                       }
                   }
               ],
               row_counts={"customers_daily.csv": len(customers_daily)}
        )
        
        # Calculate duration and end run
        duration_ms = int((time.time() - start_time) * 1000)
        
        end_run(run_id,
                status="COMPLETED", 
                duration_ms=duration_ms,
                job_namespace="ecommerce",
                job_name="etl_customers_daily",
                input_names="customers.csv,orders.csv",
                output_names="customers_daily.csv",
                transform="filter_active,hash_email,aggregate_orders,calculate_clv,categorize_engagement",
                columns_map="email_hash:email;is_high_value:total_spent+total_orders;clv_score:total_spent+total_orders+total_quantity;engagement_level:total_orders",
                rowcount_in=len(customers_df) + len(orders_df),
                rowcount_out=len(customers_daily)
        )
        
        print(f"ETL Customers Daily pipeline completed successfully. Processed {len(customers_daily)} active customers.")
        
    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        end_run(run_id,
                status="FAILED",
                duration_ms=duration_ms, 
                job_namespace="ecommerce",
                job_name="etl_customers_daily"
        )
        raise e

if __name__ == "__main__":
    run_etl_customers_daily()