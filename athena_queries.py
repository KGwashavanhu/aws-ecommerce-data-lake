import boto3
import time
import json

ATHENA = boto3.client('athena', region_name='us-east-1')
DATABASE = 'kuda_ecommerce_db'
RESULTS_BUCKET = 's3://kuda-ecommerce-datalake/analytics/query-results/'

QUERIES = {
    'revenue_by_category': """
        SELECT 
            product_category,
            COUNT(*) AS total_orders,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_order_value
        FROM curated_orders
        GROUP BY product_category
        ORDER BY total_revenue DESC
    """,
    'monthly_revenue_trend': """
        SELECT
            year,
            month,
            COUNT(*) AS total_orders,
            ROUND(SUM(total_amount), 2) AS monthly_revenue
        FROM curated_orders
        GROUP BY year, month
        ORDER BY year, CAST(month AS INT)
    """,
    'order_status_breakdown': """
        SELECT
            order_status,
            COUNT(*) AS total_orders,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage
        FROM curated_orders
        GROUP BY order_status
        ORDER BY total_orders DESC
    """,
    'top_countries_2024': """
        SELECT
            country,
            COUNT(*) AS total_orders,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM curated_orders
        WHERE year = '2024'
        GROUP BY country
        ORDER BY total_revenue DESC
    """,
    'top_5_products': """
        SELECT
            product_name,
            product_category,
            SUM(quantity) AS total_quantity_sold,
            ROUND(SUM(total_amount), 2) AS total_revenue
        FROM curated_orders
        GROUP BY product_name, product_category
        ORDER BY total_quantity_sold DESC
        LIMIT 5
    """
}

def run_query(name, sql):
    response = ATHENA.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': RESULTS_BUCKET}
    )
    execution_id = response['QueryExecutionId']
    print(f"Running: {name} ({execution_id})")

    # Poll until complete
    while True:
        status = ATHENA.get_query_execution(QueryExecutionId=execution_id)
        state = status['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(2)

    if state == 'SUCCEEDED':
        # Get data scanned
        stats = status['QueryExecution']['Statistics']
        scanned = stats.get('DataScannedInBytes', 0)
        print(f"  SUCCEEDED — scanned {scanned / 1024:.1f} KB")
        return execution_id
    else:
        reason = status['QueryExecution']['Status'].get('StateChangeReason', '')
        print(f"  {state} — {reason}")
        return None

results = {}
for name, sql in QUERIES.items():
    exec_id = run_query(name, sql)
    if exec_id:
        results[name] = exec_id

print(f"\nAll done. {len(results)}/5 queries succeeded.")
print("Results saved to:", RESULTS_BUCKET)
