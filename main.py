import sys
from src.postgres_server import postgres_server
from src.mongo_server import mongo_server
from src.neo4j_server import neo4j_server 

def main():
    # Initialize servers
    try:
        pg_server = postgres_server(host="localhost", port=5432, database="nosql_proj", user="shlok", password="shlok")
        pg_server.connect()
        m_server = mongo_server(host="localhost", port=27017, database="nosql_proj")
        m_server.connect()
        neo_server = neo4j_server(uri="bolt://localhost:7687", user="neo4j", password="neo4jpassword") 
        neo_server.connect()
    except Exception as e:
        print("Error initializing servers:", e)
        sys.exit(1)

    try:
        while True:
            command = input("Enter command ('query', 'update', 'merge', 'recover') and server ('postgres', 'mongo', 'neo4j'), followed by arguments:\n").split()
            if (len(command) == 1 and (command[0] == "exit" or command[0] == "quit")):
                break

            
            action, server_name, *args = command

            if server_name == "postgres":
                server = pg_server
            elif server_name == "mongo":
                server = m_server
            elif server_name == "neo4j":
                server = neo_server
            else:
                print("Invalid server. Please choose 'postgres', 'mongo', or 'neo4j'.")
                continue

            if action == "query":
                if len(args) != 1:
                    print("Invalid number of arguments for 'query'. Please provide a single subject.")
                    continue
                result = server.query(*args)
                if result:
                    print(f"Query result for '{args[0]}':")
                    for row in result:
                        subject, predicate, obj, timestamp = row
                        print(f"{subject}\t{predicate}\t{obj}")
                    print()
                else:
                    print(f"No results found for '{args[0]}'")

            elif action == "update":
                if len(args) != 3:
                    print("Invalid number of arguments for 'update'. Please provide subject, predicate, and new object.")
                    continue
                server.update(*args)
            elif action == "merge":
                if len(args) != 1:
                    print("Invalid number of arguments for 'merge'. Please provide the server name.")
                    continue
                if args[0] == server_name:
                    print("Server is already merged with itself.")
                    continue

                if args[0] == "postgres":
                    server2 = pg_server
                elif args[0] == "mongo":
                    server2 = m_server
                elif args[0] == "neo4j":
                    server2 = neo_server

                server.merge(*args)
                server2.merge(server_name)
            
            elif action == "recover":
                if len(args) > 0:
                    print("Invalid number of arguments for 'recover'.")
                    continue

                server.recover()

            else:
                print("Invalid action. Please choose 'query', 'update', 'merge', or 'recover'.")

    finally:
        # Disconnect servers
        pg_server.disconnect()
        m_server.disconnect()
        neo_server.disconnect()
        print("Exiting program.")

if __name__ == "__main__":
    main()
