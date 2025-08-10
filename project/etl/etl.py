#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
import pandas as pd
import psycopg2

RAW_CSV = Path(__file__).resolve().parents[2] / "data" / "raw" / "sales.csv"

PG_HOST = os.getenv("POSTGRES_HOST", "db")
PG_PORT = os.getenv("POSTGRES_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DB", "sales_db")
PG_USER = os.getenv("POSTGRES_USER", "sales_user")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "sales_pass")

def load_csv(path=RAW_CSV):
    return pd.read_csv(path)

def clean_transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=lambda c: c.strip())
    df = df[df['order_id'].notna()]
    df = df.drop_duplicates(subset=['order_id'])
    df = df.dropna(subset=['quantity', 'unit_price'])
    df['quantity'] = df['quantity'].astype(int)
    df['unit_price'] = df['unit_price'].astype(float)
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    df['total_amount'] = df['quantity'] * df['unit_price']
    df = df.sort_values(by='order_date')
    return df

def save_postgres(df: pd.DataFrame):
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()

    # Crear tabla si no existe
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        order_id INTEGER PRIMARY KEY,
        order_date DATE,
        customer_id INTEGER,
        product_id TEXT,
        quantity INTEGER,
        unit_price NUMERIC,
        region TEXT,
        total_amount NUMERIC
    );
    """)
    conn.commit()

    # Insertar datos
    for _, row in df.iterrows():
        cur.execute("""
        INSERT INTO sales (order_id, order_date, customer_id, product_id, quantity, unit_price, region, total_amount)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (order_id) DO UPDATE
        SET order_date = EXCLUDED.order_date,
            customer_id = EXCLUDED.customer_id,
            product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price,
            region = EXCLUDED.region,
            total_amount = EXCLUDED.total_amount;
        """, tuple(row))
    conn.commit()
    cur.close()
    conn.close()

def main(dry_run=False):
    df = load_csv()
    df_clean = clean_transform(df)
    if dry_run:
        print(df_clean.head(5).to_string(index=False))
        return
    save_postgres(df_clean)
    print("Datos cargados en PostgreSQL.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    main(dry_run=args.dry_run)
