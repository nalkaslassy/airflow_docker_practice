# Airflow Docker ETL Pipeline

This project contains Apache Airflow DAGs for ETL operations running in Docker containers.

## Project Structure

```
airflow_docker/
├── dags/                   # Airflow DAG files
├── scripts/                # ETL scripts (extract, transform, load)
├── config/                 # Airflow configuration
├── docker-compose.yaml     # Docker compose configuration
└── README.md              # This file
```

## Setup

1. Clone this repository
2. Copy `.env.example` to `.env` and adjust if needed
3. Set up your Airflow configuration file in `config/airflow.cfg`
4. Start the services:
   ```bash
   docker-compose up -d
   ```

## DAGs

- **day3_hello_airflow**: Basic Hello World DAG
- **week2_three_tasks**: ETL pipeline with extract, transform, and load tasks

## Configuration

Make sure to:
- Set a secure `FERNET_KEY` in your `airflow.cfg`
- Configure database connections as needed
- Review security settings before deployment

## License

This project is for learning purposes.
