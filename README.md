# Kunden ERP to Millennium API Integration

A lightweight Apache Airflow solution for automated order synchronization between Kunden ERP (Oracle) and Millennium API.

## Overview

This integration automatically extracts new orders from the Kunden ERP system and pushes them to the Millennium API, with email notifications upon completion.

## Tech Stack

- **Apache Airflow 2.8** - Workflow orchestration
- **Oracle Database** - Source ERP system
- **Python 3.8+** - DAG implementation
- **REST API** - Millennium integration

## Key Features

- **Automated Order Sync**: Scheduled DAG extracts and transfers new orders
- **Order Item Grouping**: Oracle view aggregates items by order before transmission
- **Email Notifications**: Automatic feedback emails after each execution
- **Error Handling**: Built-in retry logic and failure alerts

## Data Flow

1. Oracle view identifies new orders with grouped items
2. Airflow DAG extracts order data
3. Data is formatted and sent to Millennium API via REST
4. Execution status email sent to stakeholders

## Quick Start

```bash
# Set environment variables
export ORACLE_CONNECTION_ID=knd
export MILLENNIUM_API_URL=https://api/orders
export EMAIL_RECIPIENTS=notifications@company.com

# Deploy DAG
cp dags/kunden_millennium_sync.py $AIRFLOW_HOME/dags/

# Trigger manually or wait for schedule
airflow dags trigger kunden_millennium_sync
```

## Configuration

The DAG runs on a configurable schedule (default: hourly) and includes:
- Connection pooling for Oracle queries
- Batch processing for large order volumes
- Comprehensive logging for troubleshooting

## Monitoring

- View execution history in Airflow UI: `http://localhost:8080`
- Email reports include: processed orders count, success/failure status, and execution time

---

*Built with Apache Airflow 2.8 for reliable enterprise integration*
