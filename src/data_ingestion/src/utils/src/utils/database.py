"""
Enterprise Database Connection Module

Professional database connection management with connection pooling,
transaction handling, error recovery, and performance monitoring.

Author: Data Engineering Team
Created: 2024
"""

import logging
import time
from contextlib import contextmanager
from typing import Dict, List, Any, Optional, Generator, Tuple
import pandas as pd
from sqlalchemy import (
    create_engine, text, MetaData, Table, Column, Integer, 
    String, Float, DateTime, Boolean, inspect
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool
from dataclasses import asdict
import psycopg2
from psycopg2.extras import execute_values

from .config import get_config, DatabaseConfig


class DatabaseManager:
    """
    Enterprise-grade database manager with connection pooling,
    transaction management, and comprehensive error handling.
    """
    
    def __init__(self, config: Optional[DatabaseConfig] = None):
        """
        Initialize database manager with configuration
        
        Args:
            config: Database configuration object
        """
        self.config = config or get_config().database
        self.engine: Optional[Engine] = None
        self.metadata = MetaData()
        self.logger = self._setup_logging()
        
        # Performance tracking
        self.query_count = 0
        self.total_query_time = 0.0
        
        # Initialize connection
        self._initialize_connection()
        
        # Create tables if they don't exist
        self._create_tables()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup database-specific logging"""
        logger = logging.getLogger(f"{__name__}.DatabaseManager")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _initialize_connection(self) -> None:
        """Initialize database engine with connection pooling"""
        try:
            # Create engine with connection pooling
            self.engine = create_engine(
                self.config.connection_url,
                poolclass=QueuePool,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_pre_ping=True,  # Validate connections before use
                pool_recycle=3600,   # Recycle connections every hour
                echo=False,          # Set to True for SQL query logging
                connect_args={
                    "connect_timeout": 30,
                    "application_name": "ecommerce_analytics"
                }
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            self.logger.info("Database connection initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database connection: {str(e)}")
            raise
    
    def _create_tables(self) -> None:
        """Create database tables if they don't exist"""
        try:
            # Products table
            products_table = Table(
                'products',
                self.metadata,
                Column('id', Integer, primary_key=True),
                Column('title', String(255), nullable=False),
                Column('price', Float, nullable=False),
                Column('description', String(2000)),
                Column('category', String(100), nullable=False),
                Column('image_url', String(500)),
                Column('rating', Float),
                Column('rating_count', Integer),
                Column('ingestion_timestamp', DateTime, nullable=False),
                Column('created_at', DateTime, server_default='NOW()'),
                Column('updated_at', DateTime, server_default='NOW()'),
                extend_existing=True
            )
            
            # Data quality metrics table
            data_quality_table = Table(
                'data_quality_metrics',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('table_name', String(100), nullable=False),
                Column('total_records', Integer),
                Column('duplicate_count', Integer),
                Column('null_count', Integer),
                Column('quality_score', Float),
                Column('check_timestamp', DateTime, server_default='NOW()'),
                extend_existing=True
            )
            
            # Pipeline execution log table
            pipeline_log_table = Table(
                'pipeline_execution_log',
                self.metadata,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('pipeline_name', String(100), nullable=False),
                Column('execution_status', String(20), nullable=False),
                Column('records_processed', Integer),
                Column('execution_time_seconds', Float),
                Column('error_message', String(1000)),
                Column('started_at', DateTime, nullable=False),
                Column('completed_at', DateTime),
                extend_existing=True
            )
            
            # Create all tables
            self.metadata.create_all(self.engine)
            self.logger.info("Database tables created/verified successfully")
            
        except Exception as e:
            self.logger.error(f"Error creating database tables: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self) -> Generator:
        """
        Context manager for database connections with automatic cleanup
        
        Yields:
            Database connection object
        """
        connection = None
        try:
            connection = self.engine.connect()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            self.logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    @contextmanager
    def get_transaction(self) -> Generator:
        """
        Context manager for database transactions with automatic commit/rollback
        
        Yields:
            Database connection with active transaction
        """
        connection = None
        transaction = None
        try:
            connection = self.engine.connect()
            transaction = connection.begin()
            yield connection
            transaction.commit()
        except Exception as e:
            if transaction:
                transaction.rollback()
            self.logger.error(f"Transaction error: {str(e)}")
            raise
        finally:
            if connection:
                connection.close()
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict]:
        """
        Execute a SELECT query and return results as list of dictionaries
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries representing query results
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                result = conn.execute(text(query), params or {})
                
                # Convert result to list of dictionaries
                columns = result.keys()
                rows = result.fetchall()
                
                data = [dict(zip(columns, row)) for row in rows]
                
                # Update performance metrics
                execution_time = time.time() - start_time
                self.query_count += 1
                self.total_query_time += execution_time
                
                self.logger.debug(f"Query executed in {execution_time:.3f}s, returned {len(data)} rows")
                
                return data
                
        except Exception as e:
            self.logger.error(f"Query execution failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Params: {params}")
            raise
    
    def execute_non_query(self, query: str, params: Dict[str, Any] = None) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            Number of affected rows
        """
        try:
            with self.get_transaction() as conn:
                result = conn.execute(text(query), params or {})
                affected_rows = result.rowcount
                
                self.logger.debug(f"Non-query executed, {affected_rows} rows affected")
                
                return affected_rows
                
        except Exception as e:
            self.logger.error(f"Non-query execution failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            self.logger.error(f"Params: {params}")
            raise
    
    def bulk_insert_products(self, products: List[Dict[str, Any]]) -> int:
        """
        Efficiently insert multiple products using bulk operations
        
        Args:
            products: List of product dictionaries
            
        Returns:
            Number of inserted records
        """
        if not products:
            self.logger.warning("No products provided for bulk insert")
            return 0
        
        try:
            with self.get_transaction() as conn:
                # Prepare data for insertion
                insert_query = """
                INSERT INTO products (
                    id, title, price, description, category, 
                    image_url, rating, rating_count, ingestion_timestamp
                ) VALUES (
                    :id, :title, :price, :description, :category,
                    :image_url, :rating, :rating_count, :ingestion_timestamp
                )
                ON CONFLICT (id) DO UPDATE SET
                    title = EXCLUDED.title,
                    price = EXCLUDED.price,
                    description = EXCLUDED.description,
                    category = EXCLUDED.category,
                    image_url = EXCLUDED.image_url,
                    rating = EXCLUDED.rating,
                    rating_count = EXCLUDED.rating_count,
                    ingestion_timestamp = EXCLUDED.ingestion_timestamp,
                    updated_at = NOW()
                """
                
                result = conn.execute(text(insert_query), products)
                inserted_count = len(products)  # For upsert, this is the processed count
                
                self.logger.info(f"Bulk inserted/updated {inserted_count} products")
                
                return inserted_count
                
        except Exception as e:
            self.logger.error(f"Bulk insert failed: {str(e)}")
            raise
    
    def insert_data_quality_metrics(self, metrics: Dict[str, Any]) -> None:
        """
        Insert data quality metrics into the database
        
        Args:
            metrics: Data quality metrics dictionary
        """
        try:
            insert_query = """
            INSERT INTO data_quality_metrics (
                table_name, total_records, duplicate_count, 
                null_count, quality_score
            ) VALUES (
                :table_name, :total_records, :duplicate_count,
                :null_count, :quality_score
            )
            """
            
            self.execute_non_query(insert_query, metrics)
            self.logger.info(f"Inserted data quality metrics for {metrics.get('table_name')}")
            
        except Exception as e:
            self.logger.error(f"Failed to insert data quality metrics: {str(e)}")
            raise
    
    def log_pipeline_execution(self, pipeline_name: str, status: str, 
                             records_processed: int = 0, execution_time: float = 0.0,
                             error_message: str = None, started_at: str = None) -> None:
        """
        Log pipeline execution details
        
        Args:
            pipeline_name: Name of the executed pipeline
            status: Execution status (SUCCESS, FAILED, RUNNING)
            records_processed: Number of records processed
            execution_time: Execution time in seconds
            error_message: Error message if failed
            started_at: Start timestamp
        """
        try:
            log_data = {
                'pipeline_name': pipeline_name,
                'execution_status': status,
                'records_processed': records_processed,
                'execution_time_seconds': execution_time,
                'error_message': error_message,
                'started_at': started_at or 'NOW()'
            }
            
            if status in ['SUCCESS', 'FAILED']:
                log_data['completed_at'] = 'NOW()'
            
            insert_query = """
            INSERT INTO pipeline_execution_log (
                pipeline_name, execution_status, records_processed,
                execution_time_seconds, error_message, started_at, completed_at
            ) VALUES (
                :pipeline_name, :execution_status, :records_processed,
                :execution_time_seconds, :error_message, 
                COALESCE(:started_at::timestamp, NOW()),
                CASE WHEN :execution_status IN ('SUCCESS', 'FAILED') THEN NOW() ELSE NULL END
            )
            """
            
            self.execute_non_query(insert_query, log_data)
            self.logger.info(f"Logged pipeline execution: {pipeline_name} - {status}")
            
        except Exception as e:
            self.logger.error(f"Failed to log pipeline execution: {str(e)}")
            # Don't raise exception here to avoid breaking the main pipeline
    
    def get_table_statistics(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a database table
        
        Args:
            table_name: Name of the table to analyze
            
        Returns:
            Dictionary containing table statistics
        """
        try:
            stats_query = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT id) as unique_ids,
                MIN(created_at) as oldest_record,
                MAX(created_at) as newest_record,
                AVG(CASE WHEN price > 0 THEN price END) as avg_price,
                COUNT(CASE WHEN rating IS NOT NULL THEN 1 END) as records_with_rating
            FROM {table_name}
            WHERE created_at >= NOW() - INTERVAL '24 hours'
            """
            
            result = self.execute_query(stats_query)
            
            if result:
                stats = result[0]
                self.logger.info(f"Retrieved statistics for table {table_name}")
                return stats
            else:
                return {}
                
        except Exception as e:
            self.logger.error(f"Failed to get table statistics: {str(e)}")
            raise
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """
        Get database manager performance metrics
        
        Returns:
            Dictionary containing performance metrics
        """
        avg_query_time = (
            self.total_query_time / self.query_count 
            if self.query_count > 0 else 0
        )
        
        return {
            'total_queries_executed': self.query_count,
            'total_query_time_seconds': round(self.total_query_time, 3),
            'average_query_time_seconds': round(avg_query_time, 3),
            'connection_pool_size': self.config.pool_size,
            'max_overflow': self.config.max_overflow
        }
    
    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check
        
        Returns:
            Dictionary containing health check results
        """
        health_status = {
            'database_connection': False,
            'table_access': False,
            'write_capability': False,
            'error_message': None
        }
        
        try:
            # Test basic connection
            with self.get_connection() as conn:
                conn.execute(text("SELECT 1"))
                health_status['database_connection'] = True
            
            # Test table access
            result = self.execute_query("SELECT COUNT(*) as count FROM products LIMIT 1")
            if result:
                health_status['table_access'] = True
            
            # Test write capability (insert a test record and delete it)
            test_query = """
            INSERT INTO pipeline_execution_log (pipeline_name, execution_status, records_processed, started_at) 
            VALUES ('health_check', 'SUCCESS', 0, NOW())
            """
            self.execute_non_query(test_query)
            health_status['write_capability'] = True
            
            self.logger.info("Database health check completed successfully")
            
        except Exception as e:
            error_msg = str(e)
            health_status['error_message'] = error_msg
            self.logger.error(f"Database health check failed: {error_msg}")
        
        return health_status
    
    def close_connections(self) -> None:
        """Close all database connections"""
        try:
            if self.engine:
                self.engine.dispose()
                self.logger.info("Database connections closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing database connections: {str(e)}")


def main():
    """Demonstration of database manager functionality"""
    try:
        print("Initializing Database Manager...")
        db_manager = DatabaseManager()
        
        print("\n=== DATABASE HEALTH CHECK ===")
        health_status = db_manager.health_check()
        
        for check, status in health_status.items():
            if check != 'error_message':
                status_text = "✓ PASS" if status else "✗ FAIL"
                print(f"{check}: {status_text}")
        
        if health_status.get('error_message'):
            print(f"Error: {health_status['error_message']}")
        
        print("\n=== PERFORMANCE METRICS ===")
        metrics = db_manager.get_performance_metrics()
        
        for metric, value in metrics.items():
            print(f"{metric}: {value}")
        
        print("\n=== TABLE STATISTICS ===")
        try:
            stats = db_manager.get_table_statistics('products')
            for stat, value in stats.items():
                print(f"{stat}: {value}")
        except Exception as e:
            print(f"Could not retrieve table statistics: {str(e)}")
        
        return db_manager
        
    except Exception as e:
        logging.error(f"Database manager demonstration failed: {str(e)}")
        raise


if __name__ == "__main__":
    database_manager = main()
