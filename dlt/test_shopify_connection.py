#!/usr/bin/env python3
"""
Test script to verify Shopify API connection
"""

import dlt
import requests
from datetime import datetime

def test_shopify_connection():
    """
    Test the Shopify API connection using stored credentials
    """
    try:
        # # Get credentials from DLT secrets
        # api_token = dlt.secrets["shopify_api_token"]
        # shop_url = dlt.secrets["shopify_shop_url"]
        # Get credentials from Orchestra secrets
        api_token = os.getenv('API_TOKEN')
        store_id = os.getenv('STORE_ID')
        shop_url = f"{store_id}.myshopify.com"

        print(f"Testing connection to: {shop_url}")
        print(f"API Token: {api_token[:10]}...{api_token[-4:]} (truncated for security)")
        
        headers = {
            'X-Shopify-Access-Token': api_token,
            'Content-Type': 'application/json'
        }
        
        base_url = f"https://{shop_url}/admin/api/2024-01"
        
        # Test 1: Get shop information
        print("\n1. Testing shop information...")
        response = requests.get(f"{base_url}/shop.json", headers=headers)
        response.raise_for_status()
        
        shop_data = response.json()
        shop = shop_data.get('shop', {})
        print(f"✓ Shop name: {shop.get('name', 'N/A')}")
        print(f"✓ Shop domain: {shop.get('domain', 'N/A')}")
        print(f"✓ Shop email: {shop.get('email', 'N/A')}")
        
        # Test 2: Get orders count
        print("\n2. Testing orders access...")
        response = requests.get(f"{base_url}/orders/count.json", headers=headers)
        response.raise_for_status()
        
        orders_count = response.json().get('count', 0)
        print(f"✓ Total orders: {orders_count}")
        
        # Test 3: Get products count
        print("\n3. Testing products access...")
        response = requests.get(f"{base_url}/products/count.json", headers=headers)
        response.raise_for_status()
        
        products_count = response.json().get('count', 0)
        print(f"✓ Total products: {products_count}")
        
        # Test 4: Get customers count
        print("\n4. Testing customers access...")
        response = requests.get(f"{base_url}/customers/count.json", headers=headers)
        response.raise_for_status()
        
        customers_count = response.json().get('count', 0)
        print(f"✓ Total customers: {customers_count}")
        
        # Test 5: Get a sample order (if any exist)
        if orders_count > 0:
            print("\n5. Testing sample order retrieval...")
            response = requests.get(f"{base_url}/orders.json?limit=1", headers=headers)
            response.raise_for_status()
            
            orders_data = response.json()
            if orders_data.get('orders'):
                order = orders_data['orders'][0]
                print(f"✓ Sample order ID: {order.get('id')}")
                print(f"✓ Sample order number: {order.get('order_number')}")
                print(f"✓ Sample order total: {order.get('total_price')}")
        
        print(f"\n✓ All tests passed! Your Shopify API connection is working correctly.")
        print(f"✓ You can now run the full pipeline with: python shopify_pipeline_pipeline.py")
        
        return True
        
    except KeyError as e:
        print(f"✗ Configuration error: {e}")
        print("Please check your .dlt/secrets.toml file and ensure all required credentials are set.")
        return False
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print(f"✗ Authentication error (401): Invalid API token or insufficient permissions")
            print("Please check your API token and ensure it has the required scopes:")
            print("- read_orders")
            print("- read_products") 
            print("- read_customers")
        elif e.response.status_code == 404:
            print(f"✗ Not found error (404): Invalid shop URL")
            print("Please check your shop URL in .dlt/secrets.toml")
        else:
            print(f"✗ HTTP error: {e}")
        return False
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error: {e}")
        print("Please check your internet connection and try again.")
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    print("Testing Shopify API Connection...")
    print("=" * 50)
    
    success = test_shopify_connection()
    
    print("=" * 50)
    if success:
        print("✓ Connection test passed! You're ready to run the DLT pipeline.")
    else:
        print("✗ Connection test failed. Please fix the issues above and try again.") 