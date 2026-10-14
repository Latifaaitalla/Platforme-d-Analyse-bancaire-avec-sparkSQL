import os
# Fix pour Windows : Définir HADOOP_HOME et l'ajouter au PATH pour que Spark trouve hadoop.dll
HADOOP_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "hadoop")
os.environ["HADOOP_HOME"] = HADOOP_DIR
os.environ["PATH"] = os.path.join(HADOOP_DIR, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session():
    builder = SparkSession.builder \
        .appName("Banking_Bronze_Ingestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    # Cette méthode va télécharger automatiquement les bons JARs pour Delta Lake
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def ingest_to_bronze(spark, raw_dir, bronze_dir, table_name):
    print(f"Ingestion de la table '{table_name}' vers la couche Bronze...")
    raw_path = os.path.join(raw_dir, f"{table_name}.csv")
    bronze_path = os.path.join(bronze_dir, table_name)
    
    # Lecture du fichier CSV brut
    # Dans la couche Bronze, on garde les données telles quelles (raw -> bronze)
    df = spark.read.csv(raw_path, header=True, inferSchema=True)
    
    # Écriture au format Delta Lake avec écrasement (overwrite) en cas de relance
    df.write.format("delta").mode("overwrite").save(bronze_path)
    
    print(f"-> Succès : {df.count()} lignes sauvegardées dans {bronze_path}\n")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
    
    print("Initialisation de Spark Session avec Delta Lake...")
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    tables = ["branches", "customers", "accounts", "transactions"]
    
    for table in tables:
        ingest_to_bronze(spark, RAW_DIR, BRONZE_DIR, table)
        
    spark.stop()
    print("Ingestion Bronze terminée avec succès !")
