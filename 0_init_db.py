import psycopg2
conn = psycopg2.connect(host="localhost", database="fraud_db", user="admin", password="password")
cur = conn.cursor()
cur.execute("DROP TABLE IF EXISTS transactions_result;")
cur.execute("""
    CREATE TABLE transactions_result (
        id SERIAL PRIMARY KEY,
        tr_id INTEGER,
        montant FLOAT,
        pays VARCHAR(50),
        is_fraud INTEGER,
        timestamp TIMESTAMP
    );
""")
conn.commit()
print("✅ Base de données initialisée !")