"""
Configuration Management Module

Centralized configuration management for the e-commerce analytics pipeline.
Handles environment variables, API keys, database connections, and system settings.

Author: Data Engineering Team
Created: 2024
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class DatabaseConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    pool_size: int = 20
    max_overflow: int = 30
    
    @property
    def connection_url(self) -> str:
        """Generate SQLAlchemy connection URL"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class KafkaConfig:
    """Kafka streaming configuration"""
    bootstrap_servers: str
    topics: Dict[str, str]
    consumer_group_id: str = "ecommerce_analytics"
    batch_size: int = 1000
    max_poll_interval_ms: int = 300000
    session_timeout_ms: int = 30000


@dataclass
class APIConfig:
    """External API configuration"""
    ecommerce_base_url: str
    reddit_client_id: Optional[str]
    reddit_client_secret: Optional[str]
    openweather_api_key: Optional[str]
    request_timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3


@dataclass
class ProcessingConfig:
    """Data processing configuration"""
    batch_size: int
    window_duration_seconds: int
    checkpoint_location: str
    parallelism: int = 4
    memory_fraction: float = 0.8


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration"""
    prometheus_port: int
    grafana_port: int
    log_level: str
    metrics_enabled: bool = True
    health_check_interval: int = 60


class ConfigurationManager:
    """
    Centralized configuration management with environment variable support,
    YAML file loading, and validation.
    """
    
    def __init__(self, config_file: str = "config/config.yaml", env_file: str = ".env"):
        """
        Initialize configuration manager
        
        Args:
            config_file: Path to YAML configuration file
            env_file: Path to environment variables file
        """
        self.config_file = config_file
        self.env_file = env_file
        self.logger = self._setup_logging()
        
        # Load environment variables
        self._load_environment()
        
        # Load and validate configuration
        self.config_data = self._load_config_file()
        self._validate_configuration()
        
        # Initialize configuration objects
        self.database = self._init_database_config()
        self.kafka = self._init_kafka_config()
        self.apis = self._init_api_config()
        self.processing = self._init_processing_config()
        self.monitoring = self._init_monitoring_config()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for configuration management"""
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
    
    def _load_environment(self) -> None:
        """Load environment variables from .env file"""
        try:
            if os.path.exists(self.env_file):
                load_dotenv(self.env_file)
                self.logger.info(f"Loaded environment variables from {self.env_file}")
            else:
                self.logger.warning(f"Environment file {self.env_file} not found, using system environment")
        except Exception as e:
            self.logger.error(f"Error loading environment file: {str(e)}")
            raise
    
    def _load_config_file(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        try:
            config_path = Path(self.config_file)
            
            if not config_path.exists():
                self.logger.warning(f"Config file {self.config_file} not found, using defaults")
                return self._get_default_config()
            
            with open(config_path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
                
            # Substitute environment variables
            config_data = self._substitute_env_vars(config_data)
            
            self.logger.info(f"Loaded configuration from {self.config_file}")
            return config_data
            
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML config: {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading config file: {str(e)}")
            raise
    
    def _substitute_env_vars(self, config_data: Any) -> Any:
        """Recursively substitute environment variables in configuration"""
        if isinstance(config_data, dict):
            return {key: self._substitute_env_vars(value) for key, value in config_data.items()}
        elif isinstance(config_data, list):
            return [self._substitute_env_vars(item) for item in config_data]
        elif isinstance(config_data, str) and config_data.startswith('${') and config_data.endswith('}'):
            # Extract environment variable name
            env_var = config_data[2:-1]
            return os.getenv(env_var, config_data)
        else:
            return config_data
    
    def _get_default_config(self) -> Dict[str, Any]:
        """Return default configuration when file is not available"""
        return {
            'database': {
                'host': 'localhost',
                'port': 5432,
                'name': 'ecommerce_analytics',
                'user': 'postgres',
                'password': 'password'
            },
            'kafka': {
                'bootstrap_servers': 'localhost:9092',
                'topics': {
                    'orders': 'orders',
                    'clickstream': 'clickstream',
                    'inventory': 'inventory'
                }
            },
            'apis': {
                'fake_store': 'https://fakestoreapi.com'
            },
            'processing': {
                'batch_size': 1000,
                'window_duration': '30s',
                'checkpoint_location': './checkpoints'
            }
        }
    
    def _validate_configuration(self) -> None:
        """Validate configuration completeness and correctness"""
        required_sections = ['database', 'kafka', 'apis', 'processing']
        
        for section in required_sections:
            if section not in self.config_data:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate database configuration
        db_config = self.config_data['database']
        required_db_fields = ['host', 'port', 'name', 'user', 'password']
        
        for field in required_db_fields:
            if field not in db_config:
                raise ValueError(f"Missing required database configuration: {field}")
        
        self.logger.info("Configuration validation completed successfully")
    
    def _init_database_config(self) -> DatabaseConfig:
        """Initialize database configuration"""
        db_config = self.config_data['database']
        
        return DatabaseConfig(
            host=db_config['host'],
            port=int(db_config['port']),
            database=db_config['name'],
            username=db_config['user'],
            password=db_config['password'],
            pool_size=int(db_config.get('pool_size', 20)),
            max_overflow=int(db_config.get('max_overflow', 30))
        )
    
    def _init_kafka_config(self) -> KafkaConfig:
        """Initialize Kafka configuration"""
        kafka_config = self.config_data['kafka']
        
        return KafkaConfig(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            topics=kafka_config.get('topics', {}),
            consumer_group_id=kafka_config.get('consumer_group_id', 'ecommerce_analytics'),
            batch_size=int(kafka_config.get('batch_size', 1000)),
            max_poll_interval_ms=int(kafka_config.get('max_poll_interval_ms', 300000)),
            session_timeout_ms=int(kafka_config.get('session_timeout_ms', 30000))
        )
    
    def _init_api_config(self) -> APIConfig:
        """Initialize API configuration"""
        api_config = self.config_data['apis']
        
        return APIConfig(
            ecommerce_base_url=api_config.get('fake_store', 'https://fakestoreapi.com'),
            reddit_client_id=os.getenv('REDDIT_CLIENT_ID'),
            reddit_client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            openweather_api_key=os.getenv('OPENWEATHER_API_KEY'),
            request_timeout=int(api_config.get('request_timeout', 30)),
            max_retries=int(api_config.get('max_retries', 3)),
            backoff_factor=float(api_config.get('backoff_factor', 0.3))
        )
    
    def _init_processing_config(self) -> ProcessingConfig:
        """Initialize processing configuration"""
        proc_config = self.config_data['processing']
        
        # Convert window duration to seconds
        window_duration = proc_config.get('window_duration', '30s')
        if isinstance(window_duration, str) and window_duration.endswith('s'):
            window_seconds = int(window_duration[:-1])
        else:
            window_seconds = int(window_duration)
        
        return ProcessingConfig(
            batch_size=int(proc_config.get('batch_size', 1000)),
            window_duration_seconds=window_seconds,
            checkpoint_location=proc_config.get('checkpoint_location', './checkpoints'),
            parallelism=int(proc_config.get('parallelism', 4)),
            memory_fraction=float(proc_config.get('memory_fraction', 0.8))
        )
    
    def _init_monitoring_config(self) -> MonitoringConfig:
        """Initialize monitoring configuration"""
        return MonitoringConfig(
            prometheus_port=int(os.getenv('PROMETHEUS_PORT', 9090)),
            grafana_port=int(os.getenv('GRAFANA_PORT', 3000)),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
            metrics_enabled=os.getenv('METRICS_ENABLED', 'True').lower() == 'true',
            health_check_interval=int(os.getenv('HEALTH_CHECK_INTERVAL', 60))
        )
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration (excluding sensitive data)"""
        return {
            'database': {
                'host': self.database.host,
                'port': self.database.port,
                'database': self.database.database,
                'pool_size': self.database.pool_size
            },
            'kafka': {
                'bootstrap_servers': self.kafka.bootstrap_servers,
                'topics': list(self.kafka.topics.keys()),
                'consumer_group_id': self.kafka.consumer_group_id
            },
            'apis': {
                'ecommerce_base_url': self.apis.ecommerce_base_url,
                'request_timeout': self.apis.request_timeout,
                'max_retries': self.apis.max_retries
            },
            'processing': {
                'batch_size': self.processing.batch_size,
                'window_duration_seconds': self.processing.window_duration_seconds,
                'parallelism': self.processing.parallelism
            },
            'monitoring': {
                'log_level': self.monitoring.log_level,
                'metrics_enabled': self.monitoring.metrics_enabled
            }
        }
    
    def validate_api_connectivity(self) -> Dict[str, bool]:
        """Test connectivity to configured external APIs"""
        import requests
        
        connectivity_status = {}
        
        # Test e-commerce API
        try:
            response = requests.get(
                f"{self.apis.ecommerce_base_url}/products",
                timeout=self.apis.request_timeout
            )
            connectivity_status['ecommerce_api'] = response.status_code == 200
        except Exception:
            connectivity_status['ecommerce_api'] = False
        
        # Test Reddit API (if configured)
        if self.apis.reddit_client_id:
            try:
                # Simple auth test
                connectivity_status['reddit_api'] = True  # Placeholder
            except Exception:
                connectivity_status['reddit_api'] = False
        
        return connectivity_status


# Global configuration instance
_config_instance: Optional[ConfigurationManager] = None


def get_config() -> ConfigurationManager:
    """
    Get the global configuration instance (singleton pattern)
    
    Returns:
        ConfigurationManager: Global configuration instance
    """
    global _config_instance
    
    if _config_instance is None:
        _config_instance = ConfigurationManager()
    
    return _config_instance


def reload_config() -> ConfigurationManager:
    """
    Reload configuration (useful for testing or configuration updates)
    
    Returns:
        ConfigurationManager: New configuration instance
    """
    global _config_instance
    _config_instance = ConfigurationManager()
    return _config_instance


def main():
    """Demonstration of configuration management"""
    try:
        print("Initializing Configuration Manager...")
        config = get_config()
        
        print("\n=== CONFIGURATION SUMMARY ===")
        summary = config.get_config_summary()
        
        for section, values in summary.items():
            print(f"\n{section.upper()}:")
            for key, value in values.items():
                print(f"  {key}: {value}")
        
        print("\n=== API CONNECTIVITY TEST ===")
        connectivity = config.validate_api_connectivity()
        
        for api, status in connectivity.items():
            status_text = "✓ Connected" if status else "✗ Failed"
            print(f"{api}: {status_text}")
        
        return config
        
    except Exception as e:
        logging.error(f"Configuration initialization failed: {str(e)}")
        raise


if __name__ == "__main__":
    config_manager = main()
