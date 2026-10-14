import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

# Configuration Faker
fake = Faker('fr_FR')
Faker.seed(42)
random.seed(42)

# Constantes
NUM_CUSTOMERS = 1000
NUM_BRANCHES = 50
NUM_ACCOUNTS = 1500
NUM_TRANSACTIONS = 50000
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data", "raw")

# Création du dossier s'il n'existe pas
os.makedirs(DATA_DIR, exist_ok=True)

def generate_branches():
    branches = []
    for i in range(1, NUM_BRANCHES + 1):
        branches.append({
            "branch_id": f"BR{i:03d}",
            "branch_name": f"Agence {fake.city()}",
            "city": fake.city(),
            "country": "France"
        })
    df = pd.DataFrame(branches)
    df.to_csv(os.path.join(DATA_DIR, "branches.csv"), index=False)
    print(f"Généré {len(branches)} agences.")
    return df

def generate_customers():
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        customers.append({
            "customer_id": f"CUST{i:05d}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace('\n', ', '),
            "join_date": fake.date_between(start_date='-5y', end_date='today').isoformat()
        })
    df = pd.DataFrame(customers)
    df.to_csv(os.path.join(DATA_DIR, "customers.csv"), index=False)
    print(f"Généré {len(customers)} clients.")
    return df

def generate_accounts(customers_df, branches_df):
    accounts = []
    customer_ids = customers_df['customer_id'].tolist()
    branch_ids = branches_df['branch_id'].tolist()
    account_types = ["Courant", "Epargne", "Credit"]
    
    for i in range(1, NUM_ACCOUNTS + 1):
        accounts.append({
            "account_id": f"ACC{i:05d}",
            "customer_id": random.choice(customer_ids),
            "branch_id": random.choice(branch_ids),
            "account_type": random.choice(account_types),
            "balance": round(random.uniform(50.0, 75000.0), 2),
            "created_at": fake.date_between(start_date='-5y', end_date='today').isoformat()
        })
    df = pd.DataFrame(accounts)
    df.to_csv(os.path.join(DATA_DIR, "accounts.csv"), index=False)
    print(f"Généré {len(accounts)} comptes.")
    return df

def generate_transactions(accounts_df):
    transactions = []
    account_ids = accounts_df['account_id'].tolist()
    transaction_types = ["Depot", "Retrait", "Virement", "Paiement"]
    
    start_date = datetime.now() - timedelta(days=365)
    
    for i in range(1, NUM_TRANSACTIONS + 1):
        account_id = random.choice(account_ids)
        # Introduction d'anomalies (fraudes potentielles)
        is_fraud = random.random() < 0.03 # 3% de chances d'être une transaction anormale
        
        if is_fraud:
            # Montants très élevés
            amount = round(random.uniform(8000.0, 50000.0), 2)
            tx_type = "Retrait" if random.random() < 0.5 else "Virement"
        else:
            # Transaction normale
            amount = round(random.uniform(5.0, 1500.0), 2)
            tx_type = random.choice(transaction_types)
            
        transactions.append({
            "transaction_id": f"TXN{i:07d}",
            "account_id": account_id,
            "transaction_type": tx_type,
            "amount": amount,
            "transaction_date": fake.date_time_between(start_date=start_date, end_date='now').isoformat(),
            "channel": random.choice(["En Ligne", "DAB", "Agence", "Application Mobile"])
        })
        
    df = pd.DataFrame(transactions)
    df.to_csv(os.path.join(DATA_DIR, "transactions.csv"), index=False)
    print(f"Généré {len(transactions)} transactions.")
    return df

if __name__ == "__main__":
    print("Début de la génération des données...")
    branches_df = generate_branches()
    customers_df = generate_customers()
    accounts_df = generate_accounts(customers_df, branches_df)
    generate_transactions(accounts_df)
    print("Génération terminée ! Fichiers sauvegardés dans data/raw/")
