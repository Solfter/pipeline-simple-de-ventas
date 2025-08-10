# Proyecto ETL Simple - PostgreSQL con Docker

## Requisitos
- Docker y Docker Compose

## Estructura
- `data/raw/sales.csv`: dataset de entrada
- `project/etl/etl.py`: script ETL en Python
- `docker-compose.yml`: define PostgreSQL y contenedor ETL

## Ejecutar
1. Levantar servicios:
```bash
docker-compose up --build
