import os

# Fix Hadoop pour Windows
HADOOP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Banking_Silver_Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("1. Lecture des tables Bronze et création des vues temporaires...")
    tables = ["branches", "customers", "accounts", "transactions"]
    for table in tables:
        # On lit chaque table Bronze (format Delta)
        df = spark.read.format("delta").load(os.path.join(BRONZE_DIR, table))
        # On enregistre le DataFrame comme une table SQL virtuelle pour pouvoir l'attaquer avec Spark SQL
        df.createOrReplaceTempView(f"bronze_{table}")
        
    print("2. Nettoyage des données via Spark SQL...")
    
    # ---------------------------------------------------------
    # NETTOYAGE BRANCHES
    # ---------------------------------------------------------
    silver_branches = spark.sql("""
        SELECT 
            TRIM(branch_id) AS branch_id,
            TRIM(branch_name) AS branch_name,
            TRIM(city) AS city,
            UPPER(TRIM(country)) AS country
        FROM bronze_branches
    """)
    silver_branches.write.format("delta").mode("overwrite").save(os.path.join(SILVER_DIR, "branches"))
    print(" -> Table 'branches' nettoyée et sauvegardée.")

    # ---------------------------------------------------------
    # NETTOYAGE CUSTOMERS
    # ---------------------------------------------------------
    silver_customers = spark.sql("""
        SELECT 
            TRIM(customer_id) AS customer_id,
            INITCAP(TRIM(first_name)) AS first_name,
            INITCAP(TRIM(last_name)) AS last_name,
            LOWER(TRIM(email)) AS email,
            TRIM(phone) AS phone,
            TRIM(address) AS address,
            CAST(join_date AS DATE) AS join_date
        FROM bronze_customers
    """)
    silver_customers.write.format("delta").mode("overwrite").save(os.path.join(SILVER_DIR, "customers"))
    print(" -> Table 'customers' nettoyée et sauvegardée.")

    # ---------------------------------------------------------
    # NETTOYAGE ACCOUNTS
    # ---------------------------------------------------------
    silver_accounts = spark.sql("""
        SELECT 
            TRIM(account_id) AS account_id,
            TRIM(customer_id) AS customer_id,
            TRIM(branch_id) AS branch_id,
            UPPER(TRIM(account_type)) AS account_type,
            CAST(balance AS DECIMAL(15,2)) AS balance,
            CAST(created_at AS DATE) AS created_at
        FROM bronze_accounts
    """)
    silver_accounts.write.format("delta").mode("overwrite").save(os.path.join(SILVER_DIR, "accounts"))
    print(" -> Table 'accounts' nettoyée et sauvegardée.")

    # ---------------------------------------------------------
    # NETTOYAGE TRANSACTIONS
    # ---------------------------------------------------------
    # On ajoute une colonne de timestamp précis, et on garde la date pure pour faciliter les GROUP BY plus tard
    silver_transactions = spark.sql("""
        SELECT 
            TRIM(transaction_id) AS transaction_id,
            TRIM(account_id) AS account_id,
            UPPER(TRIM(transaction_type)) AS transaction_type,
            CAST(amount AS DECIMAL(15,2)) AS amount,
            CAST(transaction_date AS TIMESTAMP) AS transaction_timestamp,
            CAST(transaction_date AS DATE) AS transaction_date,
            UPPER(TRIM(channel)) AS channel
        FROM bronze_transactions
        WHERE amount > 0 
          AND transaction_id IS NOT NULL
    """)
    silver_transactions.write.format("delta").mode("overwrite").save(os.path.join(SILVER_DIR, "transactions"))
    print(" -> Table 'transactions' nettoyée et sauvegardée.")
    
    spark.stop()
    print("\nÉtape Silver (Nettoyage & Typage) terminée avec succès !")
