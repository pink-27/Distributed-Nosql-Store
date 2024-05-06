import psycopg2
from pymongo import MongoClient
from neo4j import GraphDatabase
import os
import shutil

# Database connection details for PostgreSQL
postgres_host = "localhost"
postgres_port = 5432
postgres_database = "nosql_proj"
postgres_user = "shlok"
postgres_password = "shlok"

# Database connection details for MongoDB
mongo_host = "localhost"
mongo_port = 27017
mongo_database = "nosql_proj"

neo4j_uri = "bolt://localhost:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4jpassword"

def connect_neo4j(uri, user, password):
    try:
        return GraphDatabase.driver(uri, auth=(user, password))
    except Exception as e:
        print("Error connecting to Neo4j:", e)
        return None

def connect_postgres():
    """Connects to the PostgreSQL database and returns a connection object."""
    conn = psycopg2.connect(host=postgres_host, port=postgres_port, database=postgres_database, user=postgres_user, password=postgres_password)
    return conn

def connect_mongo():
    """Connects to the MongoDB database and returns a database object."""
    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_database]
    return db


def initialize_log_positions():
    driver = connect_neo4j(neo4j_uri, neo4j_user, neo4j_password)
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

            # Drop all constraints using APOC procedure
            cypher_query = """
            CALL apoc.schema.assert({},{},true) YIELD label, key
            RETURN *
            """
            session.run (cypher_query)
            #session.run("CALL apoc.schema.assert({}, {})")

            session.run("CREATE INDEX FOR (s:Subject) ON (s.value)")
            session.run("CREATE INDEX FOR (r:Predicate) ON (r.value)")
            session.run("CREATE INDEX FOR (o:Object) ON (o.value)")

            # Create LogPosition constraint
            session.run("CREATE CONSTRAINT FOR (lp:LogPosition) REQUIRE lp.server_name IS UNIQUE")

            # Insert log positions
            session.run("CREATE (lp:LogPosition {server_name: 'neo4j', log_position: 0})")
            session.run("CREATE (lp:LogPosition {server_name: 'postgres', log_position: 0})")
            session.run("CREATE (lp:LogPosition {server_name: 'mongo', log_position: 0})")

    except Exception as e:
        print("Error initializing Neo4j:", e)

    conn = connect_postgres()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS triples")
            # Create the triples table
            cur.execute("""
                CREATE TABLE triples (
                    subject TEXT NOT NULL,
                    predicate TEXT NOT NULL,
                    object TEXT NOT NULL,
                    timestamp BIGINT NOT NULL,
                    PRIMARY KEY (subject, predicate)
                )
            """)

            cur.execute("CREATE INDEX ON triples (subject)")
            cur.execute("CREATE INDEX ON triples (predicate)")
            cur.execute("CREATE INDEX ON triples (object)")

            conn.commit()
            cur.close()
        except psycopg2.Error as error:
            print("Error deleting rows from 'triples' table:", error)
            conn.rollback()
        finally:
            conn.close()
    # Initialize PostgreSQL
    postgres_conn = connect_postgres()
    postgres_cur = postgres_conn.cursor()
    postgres_cur.execute("DROP TABLE IF EXISTS log_positions")
    postgres_cur.execute("CREATE TABLE log_positions (server_name TEXT PRIMARY KEY, log_position INT)")
    postgres_cur.execute("INSERT INTO log_positions (server_name, log_position) VALUES ('neo4j', 0)")
    postgres_cur.execute("INSERT INTO log_positions (server_name, log_position) VALUES ('mongo', 0)")
    postgres_cur.execute("INSERT INTO log_positions (server_name, log_position) VALUES ('postgres', 0)")  # Insert PostgreSQL position
    postgres_conn.commit()
    postgres_conn.close()

    # Initialize MongoDB
    mongo_db = connect_mongo()
    mongo_db.triples.drop()
    collection = mongo_db['triples']
    collection.create_index([("subject", 1)])  
    collection.create_index([("predicate", 1)]) 
    collection.create_index([("object", 1)])  

    mongo_db.log_positions.drop()
    mongo_db.log_positions.insert_one({"server_name": "mongo", "log_position": 0})
    mongo_db.log_positions.insert_one({"server_name": "postgres", "log_position": 0}) 
    mongo_db.log_positions.insert_one({"server_name": "neo4j", "log_position": 0}) 

if __name__ == "__main__":
    initialize_log_positions()
    print("Initialization completed successfully.")
