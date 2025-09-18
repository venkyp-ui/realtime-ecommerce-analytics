#!/usr/bin/env python3
"""
Pipeline Test Script

Simple test to validate the data pipeline functionality
without requiring full infrastructure setup.

Author: Data Engineering Team
"""

import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / 'src'))

def test_api_connectivity():
    """Test e-commerce API connectivity"""
    try:
        from data_ingestion.ecommerce_api import EcommerceAPIClient
        
        print("Testing E-commerce API connectivity...")
        client = EcommerceAPIClient()
        
        # Test categories
        categories = client.get_available_categories()
        print(f"✓ API Connected - Found {len(categories)} categories: {categories}")
        
        # Test product fetch
        products = client.get_all_products()
        print(f"✓ Data Ingestion - Retrieved {len(products)} products")
        
        # Test data quality
        df = client.products_to_dataframe(products)
        print(f"✓ Data Processing - DataFrame shape: {df.shape}")
        
        return True
        
    except Exception as e:
        print(f"✗ API Test Failed: {str(e)}")
        return False

def test_configuration():
    """Test configuration management"""
    try:
        from utils.config import get_config
        
        print("Testing configuration management...")
        config = get_config()
        
        summary = config.get_config_summary()
        print(f"✓ Configuration Loaded - {len(summary)} sections")
        
        connectivity = config.validate_api_connectivity()
        api_status = "✓ Connected" if connectivity.get('ecommerce_api') else "✗ Failed"
        print(f"✓ API Connectivity Check - {api_status}")
        
        return True
        
    except Exception as e:
        print(f"✗ Configuration Test Failed: {str(e)}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("PIPELINE VALIDATION TESTS")
    print("=" * 60)
    
    tests_passed = 0
    total_tests = 2
    
    # Test 1: API Connectivity
    if test_api_connectivity():
        tests_passed += 1
    
    print()
    
    # Test 2: Configuration
    if test_configuration():
        tests_passed += 1
    
    print("\n" + "=" * 60)
    print(f"TEST RESULTS: {tests_passed}/{total_tests} PASSED")
    
    if tests_passed == total_tests:
        print("✓ ALL TESTS PASSED - Pipeline is ready!")
    else:
        print("⚠ Some tests failed - Check configuration")
    
    print("=" * 60)
    
    return tests_passed == total_tests

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
