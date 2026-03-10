import pandas as pandas
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
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", 5432),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

# ================================================
# RUTAS DE ARCHIVOS
# ================================================

DATA_PATH = "data/raw"
FILES = {
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "payments": "olist_order_payments_dataset.csv",
    "reviews": "olist_order_reviews_dataset.csv"
}

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

    return df

def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando order_items - filas iniciales: {len(df)}")

    df = df.dropna(subset=["order_id", "product_id", "seller_id"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce").fillna(0)

    return df

def clean_order_payments(df: pd.DataFrame) -> pd.DataFrame:
    log.info(f"Limpiando order_payments - filas iniciales: {df(len)}")
    df = df.dropna(subset="order_id")
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce").fillna(1).astype(int)
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(00.00)

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
        insert_dataframe(conn, df, "dim_customers", [
            "customer_id", "customer_unique_id",
            "customer_zip_code_prefix", "customer_city", "customer_state"
        ])

        # ---PRODUCTS---
        df = pd.read_csv(DATA_PATH + FILES["products"])
        df = clean_products(df)
        insert_dataframe(conn, df, "dim_products", [
            "product_id", "product_category_name",
            "product_name_length", "product_description_length",
            "product_photos_qty", "product_weight_g",
            "product_length_cm", "product_height_cm", "product_width_cm"
        ])

        # ---SELLERS---
        df = pd.read_csv(DATA_PATH + FILES["sellers"])
        df = clean_sellers(df)
        insert_dataframe(conn, df, "dim_sellers", [
            "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
        ])

        # ---ORDERS---
        df = pd.read_csv(DATA_PATH + FILES["orders"])
        

        # ---ORDER_ITEMS---
        df = pd.read_csv(DATA_PATH + FILES["order_items"])

        # ---ORDER_PAYMENTS---
        df = pd.read_csv(DATA_PATH + FILES["payments"])

        #--- ORDER_REVIEWS---
        df = pd.read_csv(DATA_PATH + FILES["reviews"])

    except Exception as e:

