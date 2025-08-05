"""
Simple Shopify API Pipeline using DLT
Extracts orders, products, and customers from Shopify and loads into DuckDB
"""

import os
import dlt
import requests
from datetime import datetime
import time
from typing import Dict, Any, List

@dlt.source
def shopify_source():
    """
    DLT source for Shopify API
    """
    
    @dlt.resource(
        write_disposition="merge",
        primary_key="id"
    )
    def orders(limit: int = 250, status: str = "any"):
        """
        Extract orders from Shopify API
        """
        # # Get credentials from DLT secrets
        # api_token = dlt.secrets["shopify_api_token"]
        # shop_url = dlt.secrets["shopify_shop_url"]
        # Get credentials from Orchestra secrets
        api_token = os.getenv('API_TOKEN')
        store_id = os.getenv('STORE_ID')
        shop_url = f"{store_id}.myshopify.com"
        
        headers = {
            'X-Shopify-Access-Token': api_token,
            'Content-Type': 'application/json'
        }
        
        base_url = f"https://{shop_url}/admin/api/2024-01"
        
        # Get orders with pagination
        params = {
            'limit': limit,
            'status': status
        }
        
        page_info = None
        while True:
            if page_info:
                params['page_info'] = page_info
            
            try:
                response = requests.get(
                    f"{base_url}/orders.json",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                orders_data = data.get('orders', [])
                
                if not orders_data:
                    break
                
                for order in orders_data:
                    # Flatten and clean the order data
                    order_flat = {
                        'id': order.get('id'),
                        'order_number': order.get('order_number'),
                        'name': order.get('name'),
                        'email': order.get('email'),
                        'phone': order.get('phone'),
                        'created_at': order.get('created_at'),
                        'updated_at': order.get('updated_at'),
                        'processed_at': order.get('processed_at'),
                        'cancelled_at': order.get('cancelled_at'),
                        'cancel_reason': order.get('cancel_reason'),
                        'currency': order.get('currency'),
                        'financial_status': order.get('financial_status'),
                        'fulfillment_status': order.get('fulfillment_status'),
                        'total_price': order.get('total_price'),
                        'subtotal_price': order.get('subtotal_price'),
                        'total_tax': order.get('total_tax'),
                        'total_discounts': order.get('total_discounts'),
                        'total_weight': order.get('total_weight'),
                        'total_tip_received': order.get('total_tip_received'),
                        'customer_id': order.get('customer', {}).get('id') if order.get('customer') else None,
                        'customer_email': order.get('customer', {}).get('email') if order.get('customer') else None,
                        'billing_address_name': order.get('billing_address', {}).get('name') if order.get('billing_address') else None,
                        'billing_address_company': order.get('billing_address', {}).get('company') if order.get('billing_address') else None,
                        'billing_address_address1': order.get('billing_address', {}).get('address1') if order.get('billing_address') else None,
                        'billing_address_city': order.get('billing_address', {}).get('city') if order.get('billing_address') else None,
                        'billing_address_province': order.get('billing_address', {}).get('province') if order.get('billing_address') else None,
                        'billing_address_country': order.get('billing_address', {}).get('country') if order.get('billing_address') else None,
                        'billing_address_zip': order.get('billing_address', {}).get('zip') if order.get('billing_address') else None,
                        'shipping_address_name': order.get('shipping_address', {}).get('name') if order.get('shipping_address') else None,
                        'shipping_address_company': order.get('shipping_address', {}).get('company') if order.get('shipping_address') else None,
                        'shipping_address_address1': order.get('shipping_address', {}).get('address1') if order.get('shipping_address') else None,
                        'shipping_address_city': order.get('shipping_address', {}).get('city') if order.get('shipping_address') else None,
                        'shipping_address_province': order.get('shipping_address', {}).get('province') if order.get('shipping_address') else None,
                        'shipping_address_country': order.get('shipping_address', {}).get('country') if order.get('shipping_address') else None,
                        'shipping_address_zip': order.get('shipping_address', {}).get('zip') if order.get('shipping_address') else None,
                        'note': order.get('note'),
                        'tags': order.get('tags'),
                        'extracted_at': datetime.now().isoformat()
                    }
                    yield order_flat
                
                # Check for next page
                link_header = response.headers.get('Link', '')
                if 'rel="next"' in link_header:
                    # Extract page_info from Link header
                    import re
                    next_match = re.search(r'page_info=([^&>]+)', link_header)
                    if next_match:
                        page_info = next_match.group(1)
                    else:
                        break
                else:
                    break
                    
                # Rate limiting - Shopify allows 2 requests per second
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching orders: {e}")
                break
    
    @dlt.resource(
        write_disposition="merge",
        primary_key="id"
    )
    def products(limit: int = 250):
        """
        Extract products from Shopify API
        """
        # # Get credentials from DLT secrets
        # api_token = dlt.secrets["shopify_api_token"]
        # shop_url = dlt.secrets["shopify_shop_url"]
        # Get credentials from Orchestra secrets
        api_token = os.getenv('API_TOKEN')
        store_id = os.getenv('STORE_ID')
        shop_url = f"{store_id}.myshopify.com"

        headers = {
            'X-Shopify-Access-Token': api_token,
            'Content-Type': 'application/json'
        }
        
        base_url = f"https://{shop_url}/admin/api/2024-01"
        
        params = {
            'limit': limit
        }
        
        page_info = None
        while True:
            if page_info:
                params['page_info'] = page_info
            
            try:
                response = requests.get(
                    f"{base_url}/products.json",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                products_data = data.get('products', [])
                
                if not products_data:
                    break
                
                for product in products_data:
                    # Flatten and clean the product data
                    product_flat = {
                        'id': product.get('id'),
                        'title': product.get('title'),
                        'body_html': product.get('body_html'),
                        'vendor': product.get('vendor'),
                        'product_type': product.get('product_type'),
                        'created_at': product.get('created_at'),
                        'updated_at': product.get('updated_at'),
                        'published_at': product.get('published_at'),
                        'template_suffix': product.get('template_suffix'),
                        'status': product.get('status'),
                        'published_scope': product.get('published_scope'),
                        'tags': product.get('tags'),
                        'admin_graphql_api_id': product.get('admin_graphql_api_id'),
                        'handle': product.get('handle'),
                        'extracted_at': datetime.now().isoformat()
                    }
                    yield product_flat
                
                # Check for next page
                link_header = response.headers.get('Link', '')
                if 'rel="next"' in link_header:
                    import re
                    next_match = re.search(r'page_info=([^&>]+)', link_header)
                    if next_match:
                        page_info = next_match.group(1)
                    else:
                        break
                else:
                    break
                    
                # Rate limiting
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching products: {e}")
                break
    
    @dlt.resource(
        write_disposition="merge",
        primary_key="id"
    )
    def customers(limit: int = 250):
        """
        Extract customers from Shopify API
        """
        # # Get credentials from DLT secrets
        # api_token = dlt.secrets["shopify_api_token"]
        # shop_url = dlt.secrets["shopify_shop_url"]
        # Get credentials from Orchestra secrets
        api_token = os.getenv('API_TOKEN')
        store_id = os.getenv('STORE_ID')
        shop_url = f"{store_id}.myshopify.com"

        headers = {
            'X-Shopify-Access-Token': api_token,
            'Content-Type': 'application/json'
        }
        
        base_url = f"https://{shop_url}/admin/api/2024-01"
        
        params = {
            'limit': limit
        }
        
        page_info = None
        while True:
            if page_info:
                params['page_info'] = page_info
            
            try:
                response = requests.get(
                    f"{base_url}/customers.json",
                    headers=headers,
                    params=params
                )
                response.raise_for_status()
                
                data = response.json()
                customers_data = data.get('customers', [])
                
                if not customers_data:
                    break
                
                for customer in customers_data:
                    # Flatten and clean the customer data
                    customer_flat = {
                        'id': customer.get('id'),
                        'email': customer.get('email'),
                        'accepts_marketing': customer.get('accepts_marketing'),
                        'created_at': customer.get('created_at'),
                        'updated_at': customer.get('updated_at'),
                        'first_name': customer.get('first_name'),
                        'last_name': customer.get('last_name'),
                        'orders_count': customer.get('orders_count'),
                        'state': customer.get('state'),
                        'total_spent': customer.get('total_spent'),
                        'last_order_id': customer.get('last_order_id'),
                        'note': customer.get('note'),
                        'verified_email': customer.get('verified_email'),
                        'multipass_identifier': customer.get('multipass_identifier'),
                        'tax_exempt': customer.get('tax_exempt'),
                        'tags': customer.get('tags'),
                        'last_order_name': customer.get('last_order_name'),
                        'currency': customer.get('currency'),
                        'phone': customer.get('phone'),
                        'addresses': str(customer.get('addresses', [])),
                        'accepts_marketing_updated_at': customer.get('accepts_marketing_updated_at'),
                        'marketing_opt_in_level': customer.get('marketing_opt_in_level'),
                        'tax_exemptions': str(customer.get('tax_exemptions', [])),
                        'admin_graphql_api_id': customer.get('admin_graphql_api_id'),
                        'default_address': str(customer.get('default_address', {})),
                        'extracted_at': datetime.now().isoformat()
                    }
                    yield customer_flat
                
                # Check for next page
                link_header = response.headers.get('Link', '')
                if 'rel="next"' in link_header:
                    import re
                    next_match = re.search(r'page_info=([^&>]+)', link_header)
                    if next_match:
                        page_info = next_match.group(1)
                    else:
                        break
                else:
                    break
                    
                # Rate limiting
                time.sleep(0.5)
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching customers: {e}")
                break
    
    return orders, products, customers

# Main pipeline execution
if __name__ == "__main__":
    print("Running Simple Shopify API Pipeline...")
    
    # Initialize the pipeline with DuckDB destination
    pipeline = dlt.pipeline(
        pipeline_name="shopify_simple_pipeline",
        destination='duckdb',
        dataset_name="shopify_data"
    )
    
    # Run the pipeline
    source = shopify_source()
    
    # Extract orders
    print("Extracting orders...")
    orders_info = pipeline.run(
        source.orders(limit=250, status="any"),
        table_name="orders"
    )
    
    # Extract products
    print("Extracting products...")
    products_info = pipeline.run(
        source.products(limit=250),
        table_name="products"
    )
    
    # Extract customers
    print("Extracting customers...")
    customers_info = pipeline.run(
        source.customers(limit=250),
        table_name="customers"
    )
    
    print("Pipeline completed successfully!")
    print(f"Orders loaded: {orders_info}")
    print(f"Products loaded: {products_info}")
    print(f"Customers loaded: {customers_info}")
    
    print("\nYou can now query the data using DuckDB:")
    print("import duckdb")
    print("conn = duckdb.connect('shopify_data.duckdb')")
    print("orders = conn.execute('SELECT * FROM orders LIMIT 5').fetchall()")
    print("products = conn.execute('SELECT * FROM products LIMIT 5').fetchall()")
    print("customers = conn.execute('SELECT * FROM customers LIMIT 5').fetchall()")
