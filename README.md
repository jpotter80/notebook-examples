# E-commerce Data Cleaning with DuckDB
## Data Quality Assessment and Cleaning Demonstration

This repository demonstrates our systematic approach to cleaning and standardizing e-commerce product data using DuckDB. The notebook serves as a detailed walkthrough of our data cleaning methodology, showcasing how we handle common data quality challenges in e-commerce datasets.

## Overview

This demonstration tackles a common scenario in e-commerce: consolidating product data from multiple sources while ensuring data quality and consistency. We use DuckDB, a high-performance analytical database, to process data directly from various file formats without intermediate transformations.

### Source Files

The demonstration uses synthetic data that represents common e-commerce data scenarios:

1. `main_catalog.csv`: Primary product catalog containing basic product information
   - SKUs, product names, categories, base prices, inventory levels
   - Common issues: inconsistent SKU formats, missing data

2. `inventory_update.xlsx`: Recent inventory updates in Excel format
   - SKUs, current inventory levels, last update timestamps
   - Common issues: conflicts with main catalog, different date formats

3. `price_list.json`: Latest pricing information in JSON format
   - SKUs, current prices
   - Common issues: inconsistent price formats (with/without currency symbols)

4. `category_mapping.parquet`: Category standardization mapping in Parquet format
   - SKUs, standardized categories and subcategories
   - Common issues: inconsistent capitalization, missing mappings

## Data Cleaning Process

### 1. SKU Standardization
- Collects SKUs from all data sources
- Implements consistent formatting rules
- Maintains mapping between original and standardized SKUs
- Handles various format inconsistencies

### 2. Price Normalization
- Standardizes price formats across sources
- Removes currency symbols
- Converts to consistent decimal format
- Resolves conflicts between catalog and price list

### 3. Inventory Reconciliation
- Combines inventory data from multiple sources
- Implements "most recent update wins" logic
- Ensures non-negative inventory values
- Tracks inventory update timestamps

### 4. Category Standardization
- Implements consistent capitalization rules
- Resolves category hierarchies
- Handles missing subcategories
- Maintains source category mappings

## Technical Implementation

### Key Technologies
- **DuckDB**: In-process analytical SQL database
- **Python**: Primary programming language
- **Jupyter Notebook**: Interactive development and documentation
- **SQL**: Data transformation and cleaning logic

### Notable Features
1. **Direct File Processing**
   - Reads CSV, Excel, JSON, and Parquet without intermediate steps
   - Reduces memory overhead and simplifies pipeline

2. **SQL-First Approach**
   - Leverages SQL for complex data transformations
   - Maintains clarity and performance

3. **Quality Control**
   - Built-in verification steps
   - Detailed statistics at each stage
   - Data quality flags in final output

4. **Multiple Export Formats**
   - Parquet for full precision
   - CSV for broad compatibility
   - Excel for business users

## Results and Verification

The notebook includes comprehensive verification steps:

1. **Record Completeness Analysis**
   - Tracks missing data across fields
   - Calculates completion percentages
   - Identifies data quality issues

2. **Statistical Verification**
   - Price ranges and averages
   - Inventory levels and statistics
   - Category and subcategory counts

3. **Export Validation**
   - Row count verification
   - Value range checks
   - Format-specific validations

## Output Files

The process generates three versions of the cleaned dataset:

1. `cleaned_combined_products.parquet`
   - Full precision
   - Efficient storage and querying
   - Ideal for further processing

2. `cleaned_combined_products.csv`
   - Universal compatibility
   - Full precision values
   - Human-readable format

3. `cleaned_combined_products.xlsx`
   - Business-user friendly
   - Formatted for easy viewing
   - Suitable for direct use

## Usage

The notebook is designed to be both educational and practical:

1. **Educational Use**
   - Step-by-step explanations
   - Clear documentation
   - Verification at each stage

2. **Production Template**
   - Modular design
   - Configurable paths
   - Reusable functions

3. **Client Demonstration**
   - Shows methodology
   - Demonstrates capabilities
   - Highlights quality controls

## Requirements

- Python 3.12+
- DuckDB
    - spatial extension for .xlsx
- Jupyter Notebook
- Additional packages:
  - pandas
  - numpy

## License

The MIT License

