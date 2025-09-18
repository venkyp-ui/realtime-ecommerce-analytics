"""
Main Data Pipeline Execution Script

End-to-end data pipeline orchestration for real-time e-commerce analytics.
Integrates data ingestion, validation, storage, and monitoring components.

Author: Data Engineering Team
Created: 2024
"""

import logging
import time
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, List
import pandas as pd
from pathlib import Path

# Add src directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

from data_ingestion.ecommerce_api import EcommerceAPIClient, DataQualityChecker
from utils.config import get_config, ConfigurationManager
from utils.database import DatabaseManager


class DataPipeline:
    """
    Main data pipeline orchestrator that manages the complete
    end-to-end data processing workflow.
    """
    
    def __init__(self):
        """Initialize pipeline with all necessary components"""
        # Setup logging
        self.logger = self._setup_pipeline_logging()
        
        # Initialize configuration
        self.logger.info("Initializing pipeline configuration...")
        self.config = get_config()
        
        # Initialize components
        self.logger.info("Initializing pipeline components...")
        self.api_client = EcommerceAPIClient(
            base_url=self.config.apis.ecommerce_base_url,
            max_retries=self.config.apis.max_retries,
            backoff_factor=self.config.apis.backoff_factor
        )
        
        self.db_manager = DatabaseManager(self.config.database)
        self.quality_checker = DataQualityChecker()
        
        # Pipeline metrics
        self.execution_stats = {
            'pipeline_start_time': None,
            'pipeline_end_time': None,
            'total_execution_time': 0.0,
            'records_ingested': 0,
            'records_stored': 0,
            'data_quality_score': 0.0,
            'errors_encountered': 0,
            'pipeline_status': 'INITIALIZED'
        }
        
        self.logger.info("Data pipeline initialized successfully")
    
    def _setup_pipeline_logging(self) -> logging.Logger:
        """Setup comprehensive pipeline logging"""
        # Create logger
        logger = logging.getLogger('DataPipeline')
        logger.setLevel(logging.INFO)
        
        # Clear existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        # File handler (optional - creates logs directory)
        try:
            logs_dir = Path('logs')
            logs_dir.mkdir(exist_ok=True)
            
            file_handler = logging.FileHandler(
                logs_dir / f'pipeline_{datetime.now().strftime("%Y%m%d")}.log'
            )
            file_handler.setFormatter(console_formatter)
            logger.addHandler(file_handler)
        except Exception as e:
            logger.warning(f"Could not setup file logging: {str(e)}")
        
        return logger
    
    def validate_prerequisites(self) -> bool:
        """
        Validate all prerequisites before running the pipeline
        
        Returns:
            bool: True if all prerequisites are met
        """
        try:
            self.logger.info("Validating pipeline prerequisites...")
            
            # Check configuration
            config_summary = self.config.get_config_summary()
            self.logger.info("Configuration loaded successfully")
            
            # Check API connectivity
            connectivity = self.config.validate_api_connectivity()
            if not connectivity.get('ecommerce_api', False):
                self.logger.error("E-commerce API is not accessible")
                return False
            
            # Check database connectivity
            health_status = self.db_manager.health_check()
            if not all([
                health_status.get('database_connection', False),
                health_status.get('table_access', False),
                health_status.get('write_capability', False)
            ]):
                self.logger.error("Database health check failed")
                self.logger.error(f"Health status: {health_status}")
                return False
            
            self.logger.info("All prerequisites validated successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Prerequisites validation failed: {str(e)}")
            return False
    
    def ingest_product_data(self) -> pd.DataFrame:
        """
        Ingest product data from e-commerce API
        
        Returns:
            pd.DataFrame: Ingested and validated product data
        """
        try:
            self.logger.info("Starting product data ingestion...")
            
            # Fetch products from API
            products = self.api_client.get_all_products()
            self.execution_stats['records_ingested'] = len(products)
            
            if not products:
                self.logger.warning("No products retrieved from API")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = self.api_client.products_to_dataframe(products)
            
            self.logger.info(f"Successfully ingested {len(df)} products")
            return df
            
        except Exception as e:
            self.logger.error(f"Product data ingestion failed: {str(e)}")
            self.execution_stats['errors_encountered'] += 1
            raise
    
    def perform_data_quality_checks(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Perform comprehensive data quality analysis
        
        Args:
            df: Product DataFrame to analyze
            
        Returns:
            Dict containing quality metrics
        """
        try:
            self.logger.info("Performing data quality checks...")
            
            if df.empty:
                self.logger.warning("Empty DataFrame provided for quality check")
                return {'quality_score': 0.0, 'total_records': 0}
            
            # Run quality checks
            quality_metrics = self.quality_checker.check_data_quality(df)
            self.execution_stats['data_quality_score'] = quality_metrics['quality_score']
            
            # Log quality summary
            self.logger.info(f"Data Quality Score: {quality_metrics['quality_score']}/100")
            self.logger.info(f"Total Records: {quality_metrics['total_records']}")
            self.logger.info(f"Duplicate Products: {quality_metrics['duplicate_products']}")
            self.logger.info(f"Missing Values: {quality_metrics['missing_titles']}")
            
            # Store quality metrics in database
            quality_record = {
                'table_name': 'products',
                'total_records': quality_metrics['total_records'],
                'duplicate_count': quality_metrics['duplicate_products'],
                'null_count': quality_metrics['missing_titles'],
                'quality_score': quality_metrics['quality_score']
            }
            
            self.db_manager.insert_data_quality_metrics(quality_record)
            
            return quality_metrics
            
        except Exception as e:
            self.logger.error(f"Data quality checks failed: {str(e)}")
            self.execution_stats['errors_encountered'] += 1
            raise
    
    def store_product_data(self, df: pd.DataFrame) -> int:
        """
        Store product data in the database
        
        Args:
            df: Product DataFrame to store
            
        Returns:
            int: Number of records stored
        """
        try:
            self.logger.info("Storing product data in database...")
            
            if df.empty:
                self.logger.warning("Empty DataFrame provided for storage")
                return 0
            
            # Convert DataFrame to list of dictionaries for database insertion
            products_for_db = []
            
            for _, row in df.iterrows():
                product_record = {
                    'id': int(row['product_id']),
                    'title': str(row['title']),
                    'price': float(row['price']),
                    'description': str(row.get('description', '')),  # Handle if missing
                    'category': str(row['category']),
                    'image_url': str(row['image_url']),
                    'rating': float(row['rating']) if pd.notna(row['rating']) else None,
                    'rating_count': int(row['rating_count']) if pd.notna(row['rating_count']) else 0,
                    'ingestion_timestamp': row['ingestion_timestamp']
                }
                products_for_db.append(product_record)
            
            # Bulk insert products
            records_stored = self.db_manager.bulk_insert_products(products_for_db)
            self.execution_stats['records_stored'] = records_stored
            
            self.logger.info(f"Successfully stored {records_stored} product records")
            
            return records_stored
            
        except Exception as e:
            self.logger.error(f"Product data storage failed: {str(e)}")
            self.execution_stats['errors_encountered'] += 1
            raise
    
    def generate_analytics_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate business analytics summary from the data
        
        Args:
            df: Product DataFrame to analyze
            
        Returns:
            Dict containing analytics summary
        """
        try:
            self.logger.info("Generating analytics summary...")
            
            if df.empty:
                return {'message': 'No data available for analytics'}
            
            # Calculate business metrics
            analytics = {
                'total_products': len(df),
                'unique_categories': df['category'].nunique(),
                'category_distribution': df['category'].value_counts().to_dict(),
                'price_statistics': {
                    'average_price': round(df['price'].mean(), 2),
                    'median_price': round(df['price'].median(), 2),
                    'min_price': round(df['price'].min(), 2),
                    'max_price': round(df['price'].max(), 2),
                    'price_std': round(df['price'].std(), 2)
                },
                'rating_statistics': {
                    'average_rating': round(df['rating'].mean(), 2),
                    'median_rating': round(df['rating'].median(), 2),
                    'products_with_ratings': int(df['rating'].notna().sum()),
                    'total_rating_count': int(df['rating_count'].sum())
                },
                'top_categories_by_count': df['category'].value_counts().head(5).to_dict(),
                'most_expensive_category': df.groupby('category')['price'].mean().idxmax(),
                'highest_rated_category': df.groupby('category')['rating'].mean().idxmax()
            }
            
            # Log key insights
            self.logger.info(f"Analytics Summary Generated:")
            self.logger.info(f"  - Total Products: {analytics['total_products']}")
            self.logger.info(f"  - Categories: {analytics['unique_categories']}")
            self.logger.info(f"  - Avg Price: ${analytics['price_statistics']['average_price']}")
            self.logger.info(f"  - Avg Rating: {analytics['rating_statistics']['average_rating']}")
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Analytics summary generation failed: {str(e)}")
            self.execution_stats['errors_encountered'] += 1
            return {'error': str(e)}
    
    def log_pipeline_execution(self, status: str, error_message: str = None) -> None:
        """
        Log pipeline execution details to database
        
        Args:
            status: Pipeline execution status
            error_message: Error message if failed
        """
        try:
            self.db_manager.log_pipeline_execution(
                pipeline_name='ecommerce_data_ingestion',
                status=status,
                records_processed=self.execution_stats['records_stored'],
                execution_time=self.execution_stats['total_execution_time'],
                error_message=error_message,
                started_at=self.execution_stats['pipeline_start_time']
            )
        except Exception as e:
            self.logger.error(f"Failed to log pipeline execution: {str(e)}")
    
    def run_pipeline(self) -> Dict[str, Any]:
        """
        Execute the complete data pipeline
        
        Returns:
            Dict containing pipeline execution results
        """
        # Initialize execution tracking
        self.execution_stats['pipeline_start_time'] = datetime.now(timezone.utc).isoformat()
        start_time = time.time()
        
        try:
            self.logger.info("=" * 60)
            self.logger.info("STARTING E-COMMERCE DATA PIPELINE EXECUTION")
            self.logger.info("=" * 60)
            
            self.execution_stats['pipeline_status'] = 'RUNNING'
            
            # Step 1: Validate Prerequisites
            self.logger.info("Step 1: Validating Prerequisites")
            if not self.validate_prerequisites():
                raise Exception("Prerequisites validation failed")
            
            # Step 2: Data Ingestion
            self.logger.info("Step 2: Data Ingestion")
            products_df = self.ingest_product_data()
            
            if products_df.empty:
                raise Exception("No data retrieved from API")
            
            # Step 3: Data Quality Checks
            self.logger.info("Step 3: Data Quality Analysis")
            quality_metrics = self.perform_data_quality_checks(products_df)
            
            # Step 4: Data Storage
            self.logger.info("Step 4: Data Storage")
            stored_records = self.store_product_data(products_df)
            
            # Step 5: Analytics Generation
            self.logger.info("Step 5: Analytics Generation")
            analytics_summary = self.generate_analytics_summary(products_df)
            
            # Calculate execution time
            end_time = time.time()
            self.execution_stats['total_execution_time'] = round(end_time - start_time, 2)
            self.execution_stats['pipeline_end_time'] = datetime.now(timezone.utc).isoformat()
            self.execution_stats['pipeline_status'] = 'SUCCESS'
            
            # Log success
            self.log_pipeline_execution('SUCCESS')
            
            # Prepare results
            pipeline_results = {
                'execution_status': 'SUCCESS',
                'execution_stats': self.execution_stats,
                'data_quality_metrics': quality_metrics,
                'analytics_summary': analytics_summary,
                'database_performance': self.db_manager.get_performance_metrics()
            }
            
            self.logger.info("=" * 60)
            self.logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
            self.logger.info(f"Execution Time: {self.execution_stats['total_execution_time']} seconds")
            self.logger.info(f"Records Processed: {self.execution_stats['records_stored']}")
            self.logger.info(f"Data Quality Score: {self.execution_stats['data_quality_score']}/100")
            self.logger.info("=" * 60)
            
            return pipeline_results
            
        except Exception as e:
            # Handle pipeline failure
            end_time = time.time()
            self.execution_stats['total_execution_time'] = round(end_time - start_time, 2)
            self.execution_stats['pipeline_end_time'] = datetime.now(timezone.utc).isoformat()
            self.execution_stats['pipeline_status'] = 'FAILED'
            
            error_message = str(e)
            self.logger.error("=" * 60)
            self.logger.error("PIPELINE EXECUTION FAILED")
            self.logger.error(f"Error: {error_message}")
            self.logger.error(f"Execution Time: {self.execution_stats['total_execution_time']} seconds")
            self.logger.error("=" * 60)
            
            # Log failure
            self.log_pipeline_execution('FAILED', error_message)
            
            return {
                'execution_status': 'FAILED',
                'error_message': error_message,
                'execution_stats': self.execution_stats
            }
    
    def get_pipeline_status(self) -> Dict[str, Any]:
        """
        Get current pipeline status and metrics
        
        Returns:
            Dict containing current pipeline status
        """
        return {
            'current_status': self.execution_stats['pipeline_status'],
            'last_execution_time': self.execution_stats.get('pipeline_end_time'),
            'total_execution_time': self.execution_stats['total_execution_time'],
            'records_processed': self.execution_stats['records_stored'],
            'data_quality_score': self.execution_stats['data_quality_score'],
            'errors_encountered': self.execution_stats['errors_encountered']
        }


def main():
    """
    Main execution function for the data pipeline
    """
    try:
        print("Initializing Real-Time E-commerce Analytics Pipeline...")
        print("=" * 60)
        
        # Create and run pipeline
        pipeline = DataPipeline()
        results = pipeline.run_pipeline()
        
        # Display results
        print("\n" + "=" * 60)
        print("PIPELINE EXECUTION RESULTS")
        print("=" * 60)
        
        print(f"Status: {results['execution_status']}")
        
        if results['execution_status'] == 'SUCCESS':
            stats = results['execution_stats']
            print(f"Execution Time: {stats['total_execution_time']} seconds")
            print(f"Records Ingested: {stats['records_ingested']}")
            print(f"Records Stored: {stats['records_stored']}")
            print(f"Data Quality Score: {stats['data_quality_score']}/100")
            print(f"Errors: {stats['errors_encountered']}")
            
            # Display analytics summary
            if 'analytics_summary' in results:
                analytics = results['analytics_summary']
                print(f"\nBusiness Analytics:")
                print(f"  - Total Products: {analytics.get('total_products', 0)}")
                print(f"  - Categories: {analytics.get('unique_categories', 0)}")
                if 'price_statistics' in analytics:
                    price_stats = analytics['price_statistics']
                    print(f"  - Average Price: ${price_stats.get('average_price', 0)}")
                if 'rating_statistics' in analytics:
                    rating_stats = analytics['rating_statistics']
                    print(f"  - Average Rating: {rating_stats.get('average_rating', 0)}")
        else:
            print(f"Error: {results.get('error_message', 'Unknown error')}")
        
        print("=" * 60)
        
        return results
        
    except KeyboardInterrupt:
        print("\nPipeline execution interrupted by user")
        return {'execution_status': 'INTERRUPTED'}
    
    except Exception as e:
        print(f"\nPipeline execution failed: {str(e)}")
        return {'execution_status': 'FAILED', 'error_message': str(e)}


if __name__ == "__main__":
    # Execute the complete data pipeline
    pipeline_results = main()
