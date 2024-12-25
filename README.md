# ETL Toll Data Airflow Project

This project uses Apache Airflow to implement an ETL (Extract, Transform, Load) pipeline for toll data. The pipeline extracts data from various sources (CSV, TSV, and fixed-width files), transforms the data (e.g., converting text to uppercase), and loads the results into a consolidated output file.

## Project Structure

The project contains the following key components:

- **DAG File**: `ETL_toll_data.py` - The main file defining the DAG (Directed Acyclic Graph) for the ETL process.
- **Data Files**: `tolldata.tgz` - A compressed archive containing the source data files.
- **Staging Directory**: Stores transformed and consolidated data.
- **Airflow Setup**: Configurations and tasks are set up using Apache Airflow operators.

## Requirements

1. **Apache Airflow**
2. **Python 3.x**
3. **Data Files** 
4. **Bash Commands**

 **Setup Instructions**
  Place your DAG file:
    Place the `ETL_toll_data.py` file inside your Airflow `dags` directory.
## Results
**DAG Runs** : ![dag_runs](https://github.com/user-attachments/assets/5140f4ef-6401-4c16-a73b-1a1d4456e4a0)

**DAG Tasks** : ![dag_tasks](https://github.com/user-attachments/assets/1b177f0d-f7ba-4abe-bd45-f4fd9de793be)


