"""
E-commerce Data Ingestion Module

This module handles real-time data ingestion from e-commerce APIs,
implementing robust error handling, data validation, and retry mechanisms.

Author: Data Engineering Team
Created: 2024
"""

import requests
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import pandas as pd
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


@dataclass
class ProductData:
    """Data class for product information"""
    id: int
    title: str
    price: float
    description: str
    category: str
    image: str
    rating_rate: float
    rating_count: int
    ingestion_timestamp: str


class EcommerceAPIClient:
    """
    Professional e-commerce API client with robust error handling,
    retry mechanisms, and data validation.
    """
    
    def __init__(self, base_url: str = "https://fakestoreapi.com", 
                 max_retries: int = 3, backoff_factor: float = 0.3):
        """
        Initialize the API client with retry strategy
        
        Args:
            base_url: Base URL for the e-commerce API
            max_retries: Maximum number of retry attempts
            backoff_factor: Backoff factor for exponential retry
        """
        self.base_url = base_url
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Configure logging
        self.logger = self._setup_logging()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup professional logging configuration"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            
        return logger
    
    def _validate_product_data(self, product: Dict[str, Any]) -> bool:
        """
        Validate product data structure and required fields
        
        Args:
            product: Product dictionary from API response
            
        Returns:
            bool: True if valid, False otherwise
        """
        required_fields = ['id', 'title', 'price', 'description', 'category', 'image']
        
        try:
            # Check required fields exist
            for field in required_fields:
                if field not in product:
                    self.logger.error(f"Missing required field: {field}")
                    return False
            
            # Validate data types
            if not isinstance(product['id'], int):
                self.logger.error("Product ID must be integer")
                return False
                
            if not isinstance(product['price'], (int, float)):
                self.logger.error("Product price must be numeric")
                return False
                
            if product['price'] <= 0:
                self.logger.error("Product price must be positive")
                return False
                
            return True
            
        except Exception as e:
            self.logger.error(f"Validation error: {str(e)}")
            return False
    
    def get_all_products(self) -> List[ProductData]:
        """
        Fetch all products from the e-commerce API with error handling
        
        Returns:
            List[ProductData]: List of validated product data objects
        """
        try:
            self.logger.info("Starting product data ingestion...")
            
            response = self.session.get(
                f"{self.base_url}/products",
                timeout=30
            )
            response.raise_for_status()
            
            products_raw = response.json()
            self.logger.info(f"Retrieved {len(products_raw)} products from API")
            
            # Process and validate products
            validated_products = []
            current_timestamp = datetime.now(timezone.utc).isoformat()
            
            for product_raw in products_raw:
                if self._validate_product_data(product_raw):
                    try:
                        # Handle rating data safely
                        rating_data = product_raw.get('rating', {})
                        rating_rate = rating_data.get('rate', 0.0)
                        rating_count = rating_data.get('count', 0)
                        
                        product = ProductData(
                            id=product_raw['id'],
                            title=product_raw['title'][:255],  # Truncate if too long
                            price=float(product_raw['price']),
                            description=product_raw['description'][:1000],  # Truncate
                            category=product_raw['category'],
                            image=product_raw['image'],
                            rating_rate=float(rating_rate),
                            rating_count=int(rating_count),
                            ingestion_timestamp=current_timestamp
                        )
                        
                        validated_products.append(product)
                        
                    except Exception as e:
                        self.logger.error(f"Error processing product {product_raw.get('id', 'unknown')}: {str(e)}")
                        continue
                else:
                    self.logger.warning(f"Skipping invalid product: {product_raw.get('id', 'unknown')}")
            
            self.logger.info(f"Successfully processed {len(validated_products)} valid products")
            return validated_products
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise
        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse JSON response: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error during product ingestion: {str(e)}")
            raise
    
    def get_products_by_category(self, category: str) -> List[ProductData]:
        """
        Fetch products filtered by category
        
        Args:
            category: Product category to filter by
            
        Returns:
            List[ProductData]: List of products in the specified category
        """
        try:
            self.logger.info(f"Fetching products for category: {category}")
            
            response = self.session.get(
                f"{self.base_url}/products/category/{category}",
                timeout=30
            )
            response.raise_for_status()
            
            products_raw = response.json()
            self.logger.info(f"Retrieved {len(products_raw)} products for category {category}")
            
            # Process products (reuse validation logic)
            validated_products = []
            current_timestamp = datetime.now(timezone.utc).isoformat()
            
            for product_raw in products_raw:
                if self._validate_product_data(product_raw):
                    rating_data = product_raw.get('rating', {})
                    
                    product = ProductData(
                        id=product_raw['id'],
                        title=product_raw['title'][:255],
                        price=float(product_raw['price']),
                        description=product_raw['description'][:1000],
                        category=product_raw['category'],
                        image=product_raw['image'],
                        rating_rate=float(rating_data.get('rate', 0.0)),
                        rating_count=int(rating_data.get('count', 0)),
                        ingestion_timestamp=current_timestamp
                    )
                    
                    validated_products.append(product)
            
            return validated_products
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Category API request failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching category products: {str(e)}")
            raise
    
    def get_available_categories(self) -> List[str]:
        """
        Fetch all available product categories
        
        Returns:
            List[str]: List of available categories
        """
        try:
            self.logger.info("Fetching available product categories...")
            
            response = self.session.get(
                f"{self.base_url}/products/categories",
                timeout=30
            )
            response.raise_for_status()
            
            categories = response.json()
            self.logger.info(f"Found {len(categories)} categories: {categories}")
            
            return categories
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Categories API request failed: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error fetching categories: {str(e)}")
            raise
    
    def products_to_dataframe(self, products: List[ProductData]) -> pd.DataFrame:
        """
        Convert product data objects to pandas DataFrame for analysis
        
        Args:
            products: List of ProductData objects
            
        Returns:
            pd.DataFrame: Products as DataFrame
        """
        try:
            if not products:
                self.logger.warning("No products to convert to DataFrame")
                return pd.DataFrame()
            
            # Convert to dictionary format
            products_dict = []
            for product in products:
                products_dict.append({
                    'product_id': product.id,
                    'title': product.title,
                    'price': product.price,
                    'description_length': len(product.description),
                    'category': product.category,
                    'image_url': product.image,
                    'rating': product.rating_rate,
                    'rating_count': product.rating_count,
                    'ingestion_timestamp': product.ingestion_timestamp
                })
            
            df = pd.DataFrame(products_dict)
            self.logger.info(f"Created DataFrame with {len(df)} products")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error converting to DataFrame: {str(e)}")
            raise


class DataQualityChecker:
    """Data quality validation and monitoring"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def check_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality checks
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dict containing quality metrics
        """
        try:
            quality_report = {
                'total_records': len(df),
                'duplicate_products': df.duplicated(subset=['product_id']).sum(),
                'missing_titles': df['title'].isnull().sum(),
                'zero_prices': (df['price'] <= 0).sum(),
                'missing_categories': df['category'].isnull().sum(),
                'avg_price': df['price'].mean(),
                'price_std': df['price'].std(),
                'categories_count': df['category'].nunique(),
                'avg_rating': df['rating'].mean(),
                'quality_score': 0.0
            }
            
            # Calculate quality score (0-100)
            total_issues = (
                quality_report['duplicate_products'] +
                quality_report['missing_titles'] +
                quality_report['zero_prices'] +
                quality_report['missing_categories']
            )
            
            if quality_report['total_records'] > 0:
                quality_score = max(0, 100 - (total_issues / quality_report['total_records'] * 100))
                quality_report['quality_score'] = round(quality_score, 2)
            
            self.logger.info(f"Data quality score: {quality_report['quality_score']}/100")
            
            return quality_report
            
        except Exception as e:
            self.logger.error(f"Error in data quality check: {str(e)}")
            raise


def main():
    """
    Main execution function demonstrating the data ingestion pipeline
    """
    try:
        # Initialize the API client
        client = EcommerceAPIClient()
        
        # Test API connectivity
        print("Testing E-commerce API connectivity...")
        categories = client.get_available_categories()
        print(f"Available categories: {categories}")
        
        # Ingest all products
        print("\nIngesting all product data...")
        products = client.get_all_products()
        print(f"Successfully ingested {len(products)} products")
        
        # Convert to DataFrame for analysis
        df = client.products_to_dataframe(products)
        print(f"Created DataFrame with shape: {df.shape}")
        
        # Perform data quality analysis
        quality_checker = DataQualityChecker()
        quality_report = quality_checker.check_data_quality(df)
        
        print("\n=== DATA QUALITY REPORT ===")
        for metric, value in quality_report.items():
            print(f"{metric}: {value}")
        
        # Display sample data
        print("\n=== SAMPLE PRODUCTS ===")
        print(df.head(3).to_string())
        
        return df, quality_report
        
    except Exception as e:
        logging.error(f"Pipeline execution failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Execute the data ingestion pipeline
    products_df, report = main()
