import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
import os
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()

# ================================================
# CONFIGURACIÓN DE LOGGING
# ================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler("logs/etl_pipeline.log"), # Guardar logs en archivo independiente
        logging.StreamHandler() # Muestra logs en consola
    ]
)
log = logging.getLogger(__name__)

# ================================================
# CONFIGURACIÓN DE BASE DE DATOS
# ================================================
# Las credenciales viven en .env, nunca en el código

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# ================================================
# RUTAS DE ARCHIVOS
# ================================================

DATA_PATH = "data/raw/"
FILES = {
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv"
}

PROCESSED_PATH = "data/processed/"

def save_clean(df: pd.DataFrame, name: str):
    path = f"{PROCESSED_PATH}{name}_clean.csv"
    df.to_csv(path, index=False)
    log.info(f"Dataset limpio guardado: {path}")

# ================================================
# CONEXIÓN A POSTGRESQL
# ================================================

def get_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        log.info("PostgreSQL connection succesfully")
        return conn
    except Exception as e:
        log.error(f"Error connecting PostgreSQL has detected: {e}")
        raise

# ================================================
# FUNCIONES DE LIMPIEZA Y TRANSFORMACIÓN
# ================================================

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Cleaning customers - initial rows: {len(df)}")

    # Eliminar duplicados
    df = df.drop_duplicates(subset="customer_id")

    # Eliminar espacios en blanco y convertir a lowercase
    df["customer_city"] = df["customer_city"].str.strip().str.lower()
    df["customer_state"] = df["customer_state"].str.strip().str.lower()
    

    # Rellenar nulos no críticos
    df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].fillna("00000")

    # Eliminar filas sin ID (critico)
    before = len(df)
    df = df.dropna(subset=["customer_id", "customer_unique_id"])
    log.info(f"Filas eliminadas por nulos críticos: {before - len(df)}")

    return df

def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando products - filas iniciales: {len(df)}")

    df = df.drop_duplicates(subset="product_id")
    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    # Rellenar nulos de columnas numéricas
    numeric_cols = [
        "product_name_length", "product_description_length", "product_photos_qty",
        "product_length_cm", "product_weight_g", "product_width_cm", "product_height_cm"
    ]
    df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)

    return df

def clean_sellers(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando vendedores - filas iniciales: {len(df)}")

    df = df.drop_duplicates(subset="seller_id")
    df["seller_city"] = df["seller_city"].str.strip().str.lower()
    df["seller_state"] = df["seller_state"].str.strip().str.lower()
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].fillna(0)

    return df

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiar orders - filas iniciales: {len(df)}")
    
    df = df.drop_duplicates(subset="order_id")
    df = df.dropna(subset=["order_id", "customer_id"])

    #Convertir fechas

    date_cols = [
        "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date"
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        df[col] = df[col].where(df[col].notna(), None)

    return df

def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando order_items - filas iniciales: {len(df)}")

    df = df.dropna(subset=["order_id", "product_id", "seller_id"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce").fillna(0)

    return df

def clean_order_payments(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando order_payments - filas iniciales: {len(df)}")
    df = df.dropna(subset="order_id")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce").fillna(1).astype(int)
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(00.00)

    return df

def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando reviews - filas iniciales: {len(df)}")
    df = df.drop_duplicates(subset="review_id")
    df = df.dropna(subset=["review_id", "order_id"])

    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce")
    df = df[df["review_score"].between(1, 5)]

    df["review_comment_title"] = df["review_comment_title"].fillna("NA")
    df["review_comment_message"] = df["review_comment_message"].fillna("NA")
    df["review_creation_date"] = pd.to_datetime(df["review_creation_date"], errors="coerce").dt.date
    df["review_answer_timestamp"] = pd.to_datetime(df["review_answer_timestamp"], errors="coerce").dt.date

    return df

# ================================================
# FUNCIÓN GENÉRICA DE INSERCIÓN
# ================================================

def insert_dataframe(conn, df: pd.DataFrame, table: str, columns: list):
    """
    Inserta un data frame en PostgreSQL usando excute_values (bulk_insert).
    ON CONFLICT DO NOTHIN evita errores si ETL se ejecuta más de una vez.
    """
    if df.empty:
        log.warning(f"DataFrame vacio para {table}, se omite inserción")
        return
    
    rows = [tuple(row) for row in df[columns].itertuples(index=False)]
    cols = ", ".join(columns)
    sql = f"INSERT INTO ecommerce.{table} ({cols}) VALUES %s ON CONFLICT DO NOTHING"

    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
        log.info(f"✅ {table}: {len(rows)} filas insertadas")
    except Exception as e:
        log.error(f"❌ Error insertando en {table}: {e}")
        raise

# ================================================
# FUNCIÓN PRINCIPAL
# ================================================

def run_etl():
    log.info("=" *50)
    log.info("Iniciando ecommerce-dataset ETL pipeline")
    log.info("=" *50)

    # Guardar e iniciar conexión a la base de datos
    conn = get_connection()

    try:
        # ---CUSTOMERS---
        df = pd.read_csv(DATA_PATH + FILES["customers"])
        df = clean_customers(df)
        save_clean(df, "customers")
        insert_dataframe(conn, df, "dim_customers", [
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix", "customer_city", "customer_state"
        ])

        # ---PRODUCTS---
        df = pd.read_csv(DATA_PATH + FILES["products"])
        df = clean_products(df)
        save_clean(df, "products")
        insert_dataframe(conn, df, "dim_products", [
            "product_id", "product_category_name",
            "product_name_length", "product_description_length",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm"
        ])

        # ---SELLERS---
        df = pd.read_csv(DATA_PATH + FILES["sellers"])
        df = clean_sellers(df)
        save_clean(df, "sellers")
        insert_dataframe(conn, df, "dim_sellers", [
            "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
        ])

        # ---DIM_DATE: generada con las fechas de fact_order---
        df_orders_raw = pd.read_csv(DATA_PATH + FILES["orders"])
        dates = pd.to_datetime(
            df_orders_raw["order_purchase_timestamp"], errors="coerce"
        ).dt.date.dropna().unique()
        
        df_dates = pd.DataFrame({"date_id": dates})
        df_dates["date_id"] = pd.to_datetime(df_dates["date_id"])
        df_dates["year"] = df_dates["date_id"].dt.year
        df_dates["quarter"] = df_dates["date_id"].dt.quarter
        df_dates["month"] = df_dates["date_id"].dt.month
        df_dates["week"] = df_dates["date_id"].dt.isocalendar().week.astype(int)
        df_dates["day"] = df_dates["date_id"].dt.day
        df_dates["is_weekend"] = df_dates["date_id"].dt.dayofweek >= 5
        df_dates["date_id"] = df_dates["date_id"].dt.date

        save_clean(df_dates, "dates")

        insert_dataframe(conn, df_dates, "dim_date", [
            "date_id", "year", "quarter", "month", "week", "day", "is_weekend"
        ])        

        # ---ORDERS---
        df = clean_orders(df_orders_raw)
        save_clean(df, "orders")
        insert_dataframe(conn, df, "fact_orders", [
            "order_id", "customer_id", "order_status",
            "order_purchase_timestamp", "order_approved_at",
            "order_delivered_customer_date", "order_estimated_delivery_date"
        ])

        # ---ORDER_ITEMS---
        df = pd.read_csv(DATA_PATH + FILES["order_items"])
        df = clean_order_items(df)
        save_clean(df, "order_items")
        insert_dataframe(conn, df, "fact_order_items", [
            "order_id", "order_item_id", "product_id",
            "seller_id", "price", "freight_value"
        ])

        # ---ORDER_PAYMENTS---
        df = pd.read_csv(DATA_PATH + FILES["payments"])
        df = clean_order_payments(df)
        save_clean(df, "order_payments")
        insert_dataframe(conn, df, "fact_order_payments", [
            "order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"
        ])

        #--- ORDER_REVIEWS---
        df = pd.read_csv(DATA_PATH + FILES["reviews"])
        df = clean_reviews(df)
        save_clean(df, "order_reviews")
        insert_dataframe(conn, df, "fact_order_reviews", [
            "review_id", "order_id", "review_score",
            "review_creation_date", "review_answer_timestamp"
        ])

    except Exception as e:
        log.error(f"ETL fallido: {e}")
        raise
    finally:
        conn.close()
        log.info("Conexión cerrada")
        log.info("ETL finalizado")

if __name__ == "__main__":
    run_etl()