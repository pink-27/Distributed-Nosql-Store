from pymongo import MongoClient
from time import time
import os
import sys
from .server_interface import server


class mongo_server(server):
    def __init__(self, host, port, database):
        self.server_name = "mongo"
        self.host = host
        self.port = port
        self.database = database
        self.client = None
        self.sequence_number = None
        self.log_file = "mongo"
        self.log_file_shard = None
        self.current_log_file = None
        self.shard_value = 100
        self.log_file_exenstion = ".log"

    
    def connect(self):
        try:
            self.client = MongoClient(self.host, self.port)
            self.db = self.client[self.database]
            print("Connected to MongoDB database!")

            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            logs_dir = os.path.join(parent_dir, "logs")
            self.log_file = os.path.join(logs_dir, self.log_file)

            try:
                mongo_db = self.client[self.database]
                log_positions_collection = mongo_db["log_positions"]
                mongo_row = log_positions_collection.find_one({"server_name": "mongo"})
                if mongo_row:
                    self.sequence_number = mongo_row["log_position"]
                else:
                    self.disconnect()
                    sys.exit("Error: 'log_positions' table not initialized for MongoDB. Exiting...")

                self._update_log_shard() 

            except Exception as error:
                print("Error querying database:", error)

        except Exception as error:
            print("Error connecting to MongoDB database:", error)
    
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

                # Update the log position in the MongoDB collection
                self.db.log_positions.update_one(
                    {"server_name": self.server_name},
                    {"$set": {"log_position": self.sequence_number}}
                )
            
            if (self.sequence_number % self.shard_value == 0) :
                    self._update_log_shard()

        except Exception as error:
            print("Error writing to log file:", error)


    def query(self, subject):
        try:
            # Find the subject document by subject name
            triples = self.db.triples.find({"subject": subject})
            return [(triple["subject"], triple["predicate"], triple["object"], triple["timestamp"]) for triple in triples]
        except Exception as error:
            print("Error querying database:", error)
            return []


    def update(self, subject, predicate, new_object):
        try:
            timestamp = int(time() * 1000)
            self.db.triples.update_one({"subject": subject, "predicate": predicate}, {"$set": {"object": new_object, "timestamp": timestamp}}, upsert=True)
            self._write_to_log (subject, predicate, new_object, timestamp)
        except Exception as error:
            print("Error updating pair:", error)


    def merge(self, server_name):
        try:
            log_position = self.db.log_positions.find_one({"server_name": server_name})

            if log_position is None:
                raise ValueError(f"Log position not found for '{server_name}' server")

            log_pos = log_position.get("log_position", 0)
            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            logs_dir = os.path.join(parent_dir, "logs")
            
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

                for line in lines:
                    line_num, subject, predicate, obj, timestamp = line.strip().split("\t")
                    timestamp = int(timestamp)

                    existing_record = self.db.triples.find_one({"subject": subject, "predicate": predicate}, {"timestamp": 1})
        
                    if existing_record:
                        existing_timestamp = existing_record.get("timestamp", 0)
                        if (existing_timestamp < timestamp) :
                            self.db.triples.update_one({"subject": subject, "predicate": predicate}, {"$set": {"object": obj, "timestamp": timestamp}}, upsert=True)
                            self._write_to_log (subject, predicate, obj, timestamp)
                    else:
                        self.db.triples.update_one({"subject": subject, "predicate": predicate}, {"$set": {"object": obj, "timestamp": timestamp}}, upsert=True)
                        self._write_to_log (subject, predicate, obj, timestamp)
                        
                log_pos += len(lines)

            # Update log position for the current server
            self.db.log_positions.update_one(
                {"server_name": server_name},
                {"$set": {"log_position": log_pos}}
            )


        except Exception as e:
            import traceback
            traceback.print_exc()


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

                for line in lines:
                    line_num, subject, predicate, obj, timestamp = line.strip().split("\t")
                    timestamp = int(timestamp)

                    existing_record = self.db.triples.find_one({"subject": subject, "predicate": predicate}, {"timestamp": 1})
        
                    if existing_record:
                        existing_timestamp = existing_record.get("timestamp", 0)
                        if (existing_timestamp < timestamp) :
                            self.db.triples.update_one({"subject": subject, "predicate": predicate}, {"$set": {"object": obj, "timestamp": timestamp}}, upsert=True)
                            self._write_to_log (subject, predicate, obj, timestamp)
                    else:
                        self.db.triples.update_one({"subject": subject, "predicate": predicate}, {"$set": {"object": obj, "timestamp": timestamp}}, upsert=True)
                        self._write_to_log (subject, predicate, obj, timestamp)
                        
                log_pos += len(lines)

        except Exception as e:
            import traceback
            traceback.print_exc()

    
    def disconnect(self):
        try:
            if self.client:
                self.client.close()
                print("Disconnected from MongoDB database.")
        except Exception as error:
            print("Error disconnecting from MongoDB database:", error)
    