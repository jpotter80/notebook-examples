#!/usr/bin/env python3
"""
E-commerce Test Data Generator
Generates synthetic test data for e-commerce product catalog cleaning scenarios.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from pathlib import Path
import sys
from typing import Dict

# Project directory structure
PROJECT_ROOT = Path('~/freelance').expanduser()
RESOURCES_DIR = PROJECT_ROOT / 'resources'
SCRIPTS_DIR = RESOURCES_DIR / 'scripts'
TESTING_DIR = PROJECT_ROOT / 'testing'
SYNTHETIC_DATA_DIR = TESTING_DIR / 'synthetic-data'
TEST_RESULTS_DIR = TESTING_DIR / 'test-results'
SCENARIOS_DIR = TESTING_DIR / 'scenarios'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(TEST_RESULTS_DIR / 'data_generation.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TestDataGenerator:
    """Generates synthetic e-commerce test data with controlled quality issues."""
    
    def __init__(self, scenario_name: str = 'ecommerce'):
        self.scenario_name = scenario_name
        self.scenario_dir = SYNTHETIC_DATA_DIR / scenario_name
        self.logger = logging.getLogger(f"{__name__}.{scenario_name}")
        
        # Product categories and subcategories
        self.categories = ['Electronics', 'Home & Kitchen', 'Clothing', 'Books', 'Sports', 'Toys']
        self.subcategories = {
            'Electronics': ['Phones', 'Laptops', 'Accessories', 'Cameras', 'Audio'],
            'Home & Kitchen': ['Appliances', 'Cookware', 'Furniture', 'Decor', 'Storage'],
            'Clothing': ['Mens', 'Womens', 'Kids', 'Accessories', 'Shoes'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children', 'Reference'],
            'Sports': ['Equipment', 'Clothing', 'Accessories', 'Fitness', 'Outdoor'],
            'Toys': ['Educational', 'Games', 'Outdoor', 'Arts & Crafts', 'Building']
        }
        
        # Ensure directories exist
        self._setup_directories()

    def _setup_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        try:
            for dir_path in [SYNTHETIC_DATA_DIR, TEST_RESULTS_DIR, SCENARIOS_DIR, self.scenario_dir]:
                dir_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error(f"Failed to create directories: {e}")
            raise

    def generate_sku(self, category: str, subcategory: str, force_error: bool = False) -> str:
        """Generate a SKU with optional formatting issues."""
        if force_error or np.random.random() < 0.1:  # 10% chance of malformed SKU
            return f"{category[:2]}-{subcategory[:2]}-{np.random.randint(1000, 9999)}"
        return f"{category[:2].upper()}-{subcategory[:2].upper()}-{np.random.randint(1000, 9999)}"

    def generate_price(self, force_error: bool = False) -> float:
        """Generate a price value with optional formatting issues."""
        base_price = np.random.uniform(10, 500)
        if force_error or np.random.random() < 0.05:  # 5% chance of price formatting issue
            return f"${base_price:.2f}"  # String instead of numeric
        return base_price

    def generate_inventory(self, force_error: bool = False) -> int:
        """Generate inventory value with optional invalid values."""
        if force_error or np.random.random() < 0.08:  # 8% chance of invalid inventory
            return -np.random.randint(1, 10)  # Negative inventory
        return np.random.randint(0, 100)

    def generate_product_data(self, num_records: int = 10000) -> pd.DataFrame:
        """Generate synthetic product catalog data with controlled quality issues."""
        self.logger.info(f"Generating {num_records} product records...")
        
        np.random.seed(42)  # For reproducibility
        data = []
        
        try:
            for i in range(num_records):
                category = np.random.choice(self.categories)
                subcategory = np.random.choice(self.subcategories[category])
                
                record = {
                    'sku': self.generate_sku(category, subcategory),
                    'product_name': f"Test Product {i}{np.random.choice(['', ' (New)', ' - Latest Model', ' [Updated]'])}",
                    'category': category.lower() if np.random.random() < 0.15 else category,
                    'subcategory': subcategory,
                    'price': self.generate_price(),
                    'inventory': self.generate_inventory(),
                    'last_updated': datetime.now() - timedelta(days=np.random.randint(0, 365))
                }
                data.append(record)
            
            # Add duplicate records with variations (5% of records)
            num_duplicates = int(num_records * 0.05)
            for _ in range(num_duplicates):
                original = data[np.random.randint(0, len(data))].copy()
                original['product_name'] = original['product_name'].strip() + ' '
                if isinstance(original['price'], str):
                    price_val = float(original['price'].replace('$', ''))
                    original['price'] = f"${price_val * 1.01:.2f}"
                else:
                    original['price'] = original['price'] * 1.01
                data.append(original)
            
            df = pd.DataFrame(data)
            
            # Add missing values (7% of cells)
            mask = np.random.random(df.shape) < 0.07
            df[mask] = None
            
            self.logger.info(f"Generated {len(df)} records including duplicates")
            return df
            
        except Exception as e:
            self.logger.error(f"Error generating product data: {e}")
            raise

    def save_test_files(self, df: pd.DataFrame) -> Dict[str, Path]:
        """Save test data in multiple file formats."""
        self.logger.info("Saving test files in multiple formats...")
        
        file_paths = {}
        try:
            # Set up DuckDB connection for file creation
            conn = duckdb.connect(':memory:')
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            
            # Create temporary table with all data
            conn.execute("CREATE TABLE all_data AS SELECT * FROM df")
            
            # Main catalog in CSV
            main_catalog = self.scenario_dir / 'main_catalog.csv'
            conn.execute(f"""
                COPY (
                    SELECT * FROM all_data 
                    WHERE random() <= 0.6
                ) TO '{main_catalog}' (HEADER true)
            """)
            file_paths['main_catalog'] = main_catalog
            
            # Inventory update in Excel using spatial extension
            inventory_update = self.scenario_dir / 'inventory_update.xlsx'
            conn.execute(f"""
                COPY (
                    SELECT sku, inventory, last_updated 
                    FROM all_data 
                    WHERE random() <= 0.3
                ) TO '{inventory_update}' (FORMAT GDAL, DRIVER 'xlsx')
            """)
            file_paths['inventory'] = inventory_update
            
            # Price list in JSON
            price_list = self.scenario_dir / 'price_list.json'
            conn.execute(f"""
                COPY (
                    SELECT sku, price 
                    FROM all_data 
                    WHERE random() <= 0.4
                ) TO '{price_list}' (FORMAT JSON)
            """)
            file_paths['prices'] = price_list
            
            # Category mapping in Parquet
            category_mapping = self.scenario_dir / 'category_mapping.parquet'
            conn.execute(f"""
                COPY (
                    SELECT DISTINCT sku, category, subcategory 
                    FROM all_data
                ) TO '{category_mapping}' (FORMAT PARQUET)
            """)
            file_paths['categories'] = category_mapping
            
            conn.close()
            self.logger.info(f"Successfully saved test files to {self.scenario_dir}")
            return file_paths
            
        except Exception as e:
            self.logger.error(f"Error saving test files: {e}")
            raise

    def setup_duckdb(self) -> duckdb.DuckDBPyConnection:
        """Initialize DuckDB database with test data and quality metrics framework."""
        self.logger.info("Setting up DuckDB environment...")
        
        try:
            db_path = TEST_RESULTS_DIR / f'{self.scenario_name}.db'
            conn = duckdb.connect(str(db_path))
            
            # Install required extensions
            conn.execute("INSTALL spatial;")
            conn.execute("LOAD spatial;")
            
            # Create quality metrics tables
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quality_metrics (
                    check_id INTEGER,
                    check_name VARCHAR,
                    check_type VARCHAR,
                    severity VARCHAR,
                    pass_threshold FLOAT
                );
                
                CREATE TABLE IF NOT EXISTS quality_results (
                    check_id INTEGER,
                    run_timestamp TIMESTAMP,
                    records_checked INTEGER,
                    records_passed INTEGER,
                    pass_rate FLOAT
                );
            """)
            
            # Insert quality check definitions
            conn.execute("""
                INSERT INTO quality_metrics VALUES
                    (1, 'SKU Format Check', 'format', 'high', 0.98),
                    (2, 'Price Range Check', 'range', 'high', 0.95),
                    (3, 'Category Consistency', 'consistency', 'medium', 0.90),
                    (4, 'Inventory Validation', 'range', 'high', 0.99),
                    (5, 'Duplicate Detection', 'uniqueness', 'high', 0.98),
                    (6, 'Missing Value Check', 'completeness', 'medium', 0.93);
            """)
            
            self.logger.info(f"DuckDB environment set up at {db_path}")
            return conn
            
        except Exception as e:
            self.logger.error(f"Error setting up DuckDB: {e}")
            raise

    def save_expected_results(self) -> None:
        """Save expected results after data cleaning."""
        self.logger.info("Saving expected results...")
        
        try:
            expected = {
                'total_records': 10000,
                'unique_skus': 9500,
                'categories': len(self.categories),
                'subcategories': sum(len(subs) for subs in self.subcategories.values()),
                'price_range': {
                    'min': 10.0,
                    'max': 500.0
                },
                'inventory_range': {
                    'min': 0,
                    'max': 100
                },
                'quality_metrics': {
                    'sku_format': 0.90,
                    'price_valid': 0.95,
                    'category_consistent': 0.85,
                    'inventory_valid': 0.92,
                    'no_duplicates': 0.95,
                    'completeness': 0.93
                }
            }
            
            results_file = SCENARIOS_DIR / f'{self.scenario_name}_expected_results.json'
            with open(results_file, 'w') as f:
                json.dump(expected, f, indent=4)
                
            self.logger.info(f"Expected results saved to {results_file}")
            
        except Exception as e:
            self.logger.error(f"Error saving expected results: {e}")
            raise

def main():
    """Main execution function."""
    try:
        # Initialize generator
        generator = TestDataGenerator()
        
        # Generate test data
        df = generator.generate_product_data()
        
        # Save test files in multiple formats
        file_paths = generator.save_test_files(df)
        
        # Set up DuckDB environment
        conn = generator.setup_duckdb()
        conn.close()
        
        # Save expected results
        generator.save_expected_results()
        
        logger.info("Test data generation complete!")
        logger.info("Files created:")
        for name, path in file_paths.items():
            logger.info(f"- {name}: {path}")
            
        return 0
        
    except Exception as e:
        logger.error(f"Error in test data generation: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())