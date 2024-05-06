import psycopg2
import time
import os
import sys
import subprocess
from .server_interface import server


class postgres_server (server):
    def __init__(self, host, port, database, user, password):
        self.server_name = "postgres"
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.sequence_number = None
        self.log_file = "postgres"
        self.log_file_shard = None
        self.current_log_file = None
        self.shard_value = 100
        self.log_file_exenstion = ".log"
    
    def connect(self):
        try:
            self.conn = psycopg2.connect(host=self.host, port=self.port, database=self.database, user=self.user, password=self.password)
            print("Connected to PostgreSQL database!")

            try:
                cur = self.conn.cursor()
                cur.execute("SELECT log_position FROM log_positions WHERE server_name = 'postgres'")
                postgres_row = cur.fetchone()
                if postgres_row:
                    self.sequence_number = postgres_row[0]
                else:
                    cur.close()
                    self.disconnect()
                    sys.exit("Error: 'log_positions' table not initialized for PostgreSQL. Exiting...")


                parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
                logs_dir = os.path.join(parent_dir, "logs")
                self.log_file = os.path.join(logs_dir, self.log_file)
                self._update_log_shard ()

                cur.close()

            except psycopg2.Error as error:
                print("Error querying database:", error)

        except psycopg2.Error as error:
            print("Error connecting to PostgreSQL database:", error)
    
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

                cur = self.conn.cursor()
                cur.execute("UPDATE log_positions SET log_position = %s WHERE server_name = %s", (self.sequence_number, self.server_name))
                self.conn.commit()

                cur.close()

            if (self.sequence_number % self.shard_value == 0) :
                self._update_log_shard()
        except Exception as error:
            print("Error writing to log file:", error)
    
    def query(self, subject):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT subject, predicate, object, timestamp FROM triples WHERE subject = %s", (subject,))
            rows = cur.fetchall()
            cur.close()
            if rows:
                return rows
            else:
                return []
        except psycopg2.Error as error:
            print("Error querying database:", error)
            return []

    
    def update(self, subject, predicate, new_object):
        try:
            cur = self.conn.cursor()
            timestamp = int(time.time() * 1000)  # Current Unix Epoch Milliseconds
            cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s) ON CONFLICT (subject, predicate) DO UPDATE SET object = %s, timestamp = %s", (subject, predicate, new_object, timestamp, new_object, timestamp))
            self.conn.commit()
            self._write_to_log(subject, predicate, new_object, timestamp)
            cur.close()
        except psycopg2.Error as error:
            print("Error updating triple:", error)
            self.conn.rollback()


    def merge(self, server_name):
        try:
            cur = self.conn.cursor()
            # Fetch log position for the other server
            cur.execute("SELECT log_position FROM log_positions WHERE server_name = %s", (server_name,))
            log_position = cur.fetchone()

            if log_position is None:
                raise ValueError(f"Log position not found for '{server_name}' server")

            # Read log file from the last read position + 1
            parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
            logs_dir = os.path.join(parent_dir, "logs")
            log_pos = log_position[0]

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

                    # Check if the record exists in the PostgreSQL database
                    cur.execute("SELECT * FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
                    record = cur.fetchone()

                    # If record not found, insert it
                    if record is None:
                        # Insert the record into the database
                        cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)", (subject, predicate, obj, timestamp))
                        self.conn.commit()

                        self._write_to_log (subject, predicate, obj, timestamp)
                    else:
                        # Check if the existing record has an older timestamp
                        existing_timestamp = int(record[3])
                        if existing_timestamp < timestamp:
                            # Update the record with the newer timestamp
                            cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s) ON CONFLICT (subject, predicate) DO UPDATE SET object = %s, timestamp = %s", (subject, predicate, obj, timestamp, obj, timestamp))
                            self.conn.commit()
                            self._write_to_log (subject, predicate, obj, timestamp)

                log_pos += len(lines)
                # Update log position for the current server

            cur.execute("UPDATE log_positions SET log_position = %s WHERE server_name = %s", (log_pos, server_name))
            self.conn.commit()

        except psycopg2.Error as e:
            print("Error during merge:", e)
            self.conn.rollback()  # Rollback transaction in case of error
        except Exception as e:
            print("Unexpected error:", e)
            self.conn.rollback()  # Rollback transaction for any unexpected errors
        finally:
            if cur:
                cur.close()  # Close cursor

    def recover(self):
        try:
            cur = self.conn.cursor()
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

                    # Check if the record exists in the PostgreSQL database
                    cur.execute("SELECT * FROM triples WHERE subject = %s AND predicate = %s", (subject, predicate))
                    record = cur.fetchone()

                    # If record not found, insert it
                    if record is None:
                        # Insert the record into the database
                        cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s)", (subject, predicate, obj, timestamp))
                        self.conn.commit()

                        self._write_to_log (subject, predicate, obj, timestamp)
                    else:
                        # Check if the existing record has an older timestamp
                        existing_timestamp = int(record[3])
                        if existing_timestamp < timestamp:
                            # Update the record with the newer timestamp
                            cur.execute("INSERT INTO triples (subject, predicate, object, timestamp) VALUES (%s, %s, %s, %s) ON CONFLICT (subject, predicate) DO UPDATE SET object = %s, timestamp = %s", (subject, predicate, obj, timestamp, obj, timestamp))
                            self.conn.commit()
                            self._write_to_log (subject, predicate, obj, timestamp)

                log_pos += len(lines)
                # Update log position for the current server

        except psycopg2.Error as e:
            print("Error during recover:", e)
            self.conn.rollback()  # Rollback transaction in case of error
        except Exception as e:
            print("Unexpected error:", e)
            self.conn.rollback()  # Rollback transaction for any unexpected errors
        finally:
            if cur:
                cur.close()  # Close cursor
        
    
    def disconnect(self):
        try:
            self.conn.close()
            print("Disconnected from PostgreSQL database.")
        except psycopg2.Error as error:
            print("Error disconnecting from PostgreSQL database:", error)
