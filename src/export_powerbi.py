import os
import pandas as pd

# Fix Hadoop pour Windows
HADOOP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Export_PowerBI") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "4g") \
        .config("spark.network.timeout", "800s")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    GOLD_DIR = os.path.join(BASE_DIR, "data", "gold")
    POWERBI_DIR = os.path.join(BASE_DIR, "data", "export_powerbi")
    
    # Création du dossier d'export s'il n'existe pas
    os.makedirs(POWERBI_DIR, exist_ok=True)
    
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("Début de l'exportation des tables Gold pour Power BI...")
    
    tables_to_export = ["branch_kpi", "customer_360", "transaction_metrics", "account_performance", "fraud_analysis"]
    
    for table in tables_to_export:
        print(f" -> Export de la table {table}...")
        # 1. Lecture de la table Delta
        df_spark = spark.read.format("delta").load(os.path.join(GOLD_DIR, table))
        
        # 2. Conversion en Pandas DataFrame (car les données Gold sont agrégées et donc plus petites)
        df_pandas = df_spark.toPandas()
        
        # 3. Sauvegarde en fichier CSV unique propre pour Power BI
        export_path = os.path.join(POWERBI_DIR, f"{table}.csv")
        df_pandas.to_csv(export_path, index=False, sep=';', decimal=',')
        print(f"    Sauvegardé dans : {export_path}")

    spark.stop()
    print("\nExportation terminée ! Les fichiers sont prêts pour Power BI.")
