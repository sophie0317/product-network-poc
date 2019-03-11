import psycopg2

def connect(**kwargs):
    conn = psycopg2.connect(**kwargs)
    return conn
