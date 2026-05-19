import psycopg2

DB_HOST = "localhost"
DB_NAME = "taxidb"
DB_USER = "admin"
DB_PASS = "password"

def reset_db():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        conn.autocommit = True
        cur = conn.cursor()
        
        print("Dropping old tables to refresh schema...")
        cur.execute("DROP TABLE IF EXISTS normal_trips;")
        cur.execute("DROP TABLE IF EXISTS anomalous_trips;")
        cur.execute("DROP TABLE IF EXISTS pipeline_metrics;")
        
        print("[OK] Database reset successful. Now you can run spark_taxi_processor.py")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[ERROR] Could not reset DB: {e}")
        print("Make sure Docker containers are running (docker-compose up -d)")

if __name__ == "__main__":
    reset_db()
