import os

# Fix Hadoop pour Windows
HADOOP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Banking_Gold_Processing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.network.timeout", "800s")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")
    GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("1. Lecture des tables Silver et création des vues temporaires...")
    tables = ["branches", "customers", "accounts", "transactions"]
    for table in tables:
        # On lit chaque table Silver
        df = spark.read.format("delta").load(os.path.join(SILVER_DIR, table))
        # On enregistre la vue pour Spark SQL
        df.createOrReplaceTempView(f"silver_{table}")
        
    print("2. Création des agrégations métiers (Couche Gold) via Spark SQL...")
    
    # ---------------------------------------------------------
    # KPI AGENCES (Vue globale de la performance des agences)
    # ---------------------------------------------------------
    print(" -> Génération de 'gold_branch_kpi'...")
    gold_branch_kpi = spark.sql("""
        SELECT 
            b.branch_id,
            b.branch_name,
            b.city,
            COUNT(DISTINCT a.account_id) AS total_accounts,
            SUM(a.balance) AS total_balance_managed,
            COUNT(t.transaction_id) AS total_transaction_count,
            SUM(t.amount) AS total_transaction_volume,
            AVG(a.balance) AS avg_account_balance
        FROM silver_branches b
        LEFT JOIN silver_accounts a ON b.branch_id = a.branch_id
        LEFT JOIN silver_transactions t ON a.account_id = t.account_id
        GROUP BY b.branch_id, b.branch_name, b.city
    """)
    gold_branch_kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(GOLD_DIR, "branch_kpi"))

    # ---------------------------------------------------------
    # VUE CLIENT 360 (Résumé complet par client)
    # ---------------------------------------------------------
    print(" -> Génération de 'gold_customer_360'...")
    gold_customer_360 = spark.sql("""
        SELECT 
            c.customer_id,
            c.first_name,
            c.last_name,
            c.join_date,
            COUNT(DISTINCT a.account_id) AS total_accounts,
            SUM(a.balance) AS total_current_balance,
            COUNT(t.transaction_id) AS total_transactions,
            SUM(CASE WHEN UPPER(t.transaction_type) = 'DEPOT' THEN t.amount ELSE 0 END) AS total_deposits,
            SUM(CASE WHEN UPPER(t.transaction_type) = 'RETRAIT' THEN t.amount ELSE 0 END) AS total_withdrawals,
            CASE 
                WHEN SUM(a.balance) >= 50000 THEN 'Premium'
                WHEN SUM(a.balance) >= 20000 THEN 'Gold'
                WHEN SUM(a.balance) >= 5000 THEN 'Standard'
                ELSE 'Basic' 
            END AS customer_segment
        FROM silver_customers c
        LEFT JOIN silver_accounts a ON c.customer_id = a.customer_id
        LEFT JOIN silver_transactions t ON a.account_id = t.account_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.join_date
    """)
    gold_customer_360.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(GOLD_DIR, "customer_360"))
    
    # ---------------------------------------------------------
    # ANALYSE DES TRANSACTIONS (Temporel & Canal)
    # ---------------------------------------------------------
    print(" -> Génération de 'gold_transaction_metrics'...")
    gold_transaction_metrics = spark.sql("""
        SELECT 
            DATE(t.transaction_date) AS txn_date,
            t.transaction_type,
            t.channel,
            COUNT(t.transaction_id) AS tx_count,
            SUM(t.amount) AS total_amount,
            AVG(t.amount) AS avg_amount
        FROM silver_transactions t
        GROUP BY DATE(t.transaction_date), t.transaction_type, t.channel
    """)
    gold_transaction_metrics.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(GOLD_DIR, "transaction_metrics"))

    # ---------------------------------------------------------
    # PERFORMANCE DES COMPTES (Par type de compte)
    # ---------------------------------------------------------
    print(" -> Génération de 'gold_account_performance'...")
    gold_account_performance = spark.sql("""
        SELECT 
            a.account_type,
            b.branch_name,
            COUNT(a.account_id) AS nb_accounts,
            SUM(a.balance) AS total_balance,
            AVG(a.balance) AS avg_balance
        FROM silver_accounts a
        JOIN silver_branches b ON a.branch_id = b.branch_id
        GROUP BY a.account_type, b.branch_name
    """)
    gold_account_performance.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(GOLD_DIR, "account_performance"))

    # ---------------------------------------------------------
    # ANALYSE DES FRAUDES / RISQUES
    # ---------------------------------------------------------
    print(" -> Génération de 'gold_fraud_analysis'...")
    gold_fraud_analysis = spark.sql("""
        SELECT 
            t.transaction_id,
            DATE(t.transaction_date) AS txn_date,
            t.amount,
            t.transaction_type,
            t.channel,
            c.customer_id,
            c.first_name,
            c.last_name,
            b.branch_name,
            CASE 
                WHEN t.amount > 8000 THEN 'Critical'
                WHEN t.amount > 3000 THEN 'High'
                ELSE 'Normal' 
            END AS risk_level
        FROM silver_transactions t
        JOIN silver_accounts a ON t.account_id = a.account_id
        JOIN silver_customers c ON a.customer_id = c.customer_id
        JOIN silver_branches b ON a.branch_id = b.branch_id
        WHERE t.amount > 3000
    """)
    gold_fraud_analysis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(os.path.join(GOLD_DIR, "fraud_analysis"))
    
    spark.stop()
    print("\nÉtape Gold (Agrégations Métiers) terminée avec succès !")
