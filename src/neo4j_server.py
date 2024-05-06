from neo4j import GraphDatabase
import time
import os
import sys
import subprocess
from .server_interface import server


class neo4j_server (server):
    def __init__(self, uri, user, password):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = None
        self.sequence_number = None
        self.log_file = "neo4j"
        self.log_file_shard = None
        self.current_log_file = None
        self.shard_value = 100
        self.log_file_exenstion = ".log"


    def connect(self):
        try:
            self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
            print("Connected to Neo4j database!")

            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            logs_dir = os.path.join(parent_dir, "logs")
            self.log_file = os.path.join(logs_dir, self.log_file)

            with self.driver.session() as session:
                result = session.run("MATCH (n:LogPosition) WHERE n.server_name = 'neo4j' RETURN n.log_position AS log_position")
                neo4j_row = result.single()
                if neo4j_row:
                    self.sequence_number = neo4j_row["log_position"]
                else:
                    self.disconnect()
                    raise ValueError("Error: 'LogPosition' node not found for Neo4j.")

                self._update_log_shard()

        except Exception as e:
            print("Error connecting to Neo4j database:", e)

    
    def _update_log_shard (self) :
        self.log_file_shard = self.sequence_number // self.shard_value
        self.current_log_file = self.log_file + "_" + str(self.log_file_shard) + self.log_file_exenstion
        if os.path.exists(self.current_log_file):
                pass
        else:
            with open(self.current_log_file, "w") as file:
                pass
            
    def _write_to_log(self, subject, predicate, new_object, timestamp):
        try:
            with open(self.current_log_file, "a") as file:
                self.sequence_number += 1
                log_entry = f"{self.sequence_number}\t{subject}\t{predicate}\t{new_object}\t{timestamp}\n"
                file.write(log_entry)

                with self.driver.session() as session:
                    session.run("MATCH (n:LogPosition) WHERE n.server_name = 'neo4j' SET n.log_position = $log_position", log_position=self.sequence_number)
            
            if (self.sequence_number % self.shard_value == 0) :
                self._update_log_shard()

        except Exception as error:
            print("Error writing to log file:", error)

    
    def query(self, subject):
        try:
            with self.driver.session() as session:
                result = session.run("MATCH (s:Subject {value: $subject})-[r]->(o:Object) RETURN s.value AS subject, r.value AS predicate, o.value AS object, o.timestamp AS timestamp", subject=subject)

                rows = [(record["subject"], record["predicate"], record["object"], record["timestamp"]) for record in result]
                return rows

        except Exception as error:
            print("Error querying Neo4j database:", error)
            return []

    
    def update(self, subject, predicate, new_object):
        try:
            timestamp = int(time.time() * 1000)  # Current Unix Epoch Milliseconds
            
            with self.driver.session() as session:
                # Check if the subject and predicate already exist
                result = session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) RETURN s, r, o", subject=subject, predicate=predicate)
                record = result.single()
                
                if record:
                    session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) SET o.value = $new_object, o.timestamp = $timestamp", subject=subject, predicate=predicate, new_object=new_object, timestamp=timestamp)
                else:
                    session.run("MERGE (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o:Object {value: $new_object, timestamp: $timestamp})", subject=subject, predicate=predicate, new_object=new_object, timestamp=timestamp)

                self._write_to_log(subject, predicate, new_object, timestamp)
        except Exception as error:
            print("Error updating triple:", error)



    def merge(self, server_name):
        try:
            with self.driver.session() as session:
                result = session.run("""
                MATCH (lp:LogPosition {server_name: $server_name})
                RETURN lp.log_position AS log_position
                """, server_name=server_name)
                record = result.single()

            if record is None:
                raise ValueError(f"Log position not found for '{server_name}' server")
            log_position = record["log_position"]


            # Read log file from the last read position + 1
            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            logs_dir = os.path.join(parent_dir, "logs")
            log_pos = log_position

            while (True) :
                log_file_shard = log_pos // self.shard_value
                log_file = os.path.join(logs_dir, server_name + "_" + str(log_file_shard) + self.log_file_exenstion)
                new_log_pos = log_pos % self.shard_value

                if os.path.exists(log_file):
                    with open(log_file, "r") as f:
                        lines = f.readlines()[new_log_pos:]
                else:
                    break

                with open(log_file, "r") as f:
                    lines = f.readlines()[new_log_pos:]

                if (len(lines) == 0) :
                    break

                # Process each line from the log file
                for line in lines:
                    # Parse line to extract subject, predicate, object, and timestamp
                    line_num, subject, predicate, obj, timestamp = line.strip().split("\t")
                    timestamp = int(timestamp)

                    with self.driver.session() as session:
                        result = session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) RETURN s, r, o", subject=subject, predicate=predicate)
                        record = result.single()

                        if record is None:
                            # Insert the record into the database
                            session.run("MERGE (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o:Object {value: $new_object, timestamp: $timestamp})", subject=subject, predicate=predicate, new_object=obj, timestamp=timestamp)
                            self._write_to_log (subject, predicate, obj, timestamp)
                        else:
                            # Check if the existing record has an older timestamp
                            existing_timestamp = int(record["o"]["timestamp"])
                            if existing_timestamp < timestamp:
                                session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) SET o.value = $new_object, o.timestamp = $timestamp", subject=subject, predicate=predicate, new_object=obj, timestamp=timestamp)
                                self._write_to_log (subject, predicate, obj, timestamp)
                
                log_pos += len (lines)

            try:
                with self.driver.session() as session:
                    # Update the log position in the Neo4j database
                    session.run("""
                    MATCH (lp:LogPosition {server_name: $server_name})
                    SET lp.log_position = $log_position
                    """, server_name=server_name, log_position=log_pos)
            except Exception as e:
                print("Error updating log position:", e)
        except Exception as e:
                print("Error merging Neoj4 database:", e)


    def recover (self) :
        try :
            log_pos = 0

            while (True) :
                log_file_shard = log_pos // self.shard_value
                log_file = os.path.join(self.log_file + "_" + str(log_file_shard) + self.log_file_exenstion)
                new_log_pos = log_pos % self.shard_value

                if os.path.exists(log_file):
                    with open(log_file, "r") as f:
                        lines = f.readlines()[new_log_pos:]
                else:
                    break

                with open(log_file, "r") as f:
                    lines = f.readlines()[new_log_pos:]

                if (len(lines) == 0) :
                    break

                # Process each line from the log file
                for line in lines:
                    # Parse line to extract subject, predicate, object, and timestamp
                    line_num, subject, predicate, obj, timestamp = line.strip().split("\t")
                    timestamp = int(timestamp)

                    with self.driver.session() as session:
                        result = session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) RETURN s, r, o", subject=subject, predicate=predicate)
                        record = result.single()

                        if record is None:
                            # Insert the record into the database
                            session.run("MERGE (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o:Object {value: $new_object, timestamp: $timestamp})", subject=subject, predicate=predicate, new_object=obj, timestamp=timestamp)
                            self._write_to_log (subject, predicate, obj, timestamp)
                        else:
                            # Check if the existing record has an older timestamp
                            existing_timestamp = int(record["o"]["timestamp"])
                            if existing_timestamp < timestamp:
                                session.run("MATCH (s:Subject {value: $subject})-[r:Predicate {value: $predicate}]->(o) SET o.value = $new_object, o.timestamp = $timestamp", subject=subject, predicate=predicate, new_object=obj, timestamp=timestamp)
                                self._write_to_log (subject, predicate, obj, timestamp)
                
                log_pos += len (lines)

        except Exception as e:
                print("Error merging Neoj4 database:", e)
 
    def disconnect(self):
        try:
            self.driver.close()
            print("Disconnected from Neo4j database.")
        except Exception as error:
            print("Error disconnecting from Neo4j database:", error)
