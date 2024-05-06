import csv
import time
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.postgres_server import postgres_server
from src.mongo_server import mongo_server
from src.neo4j_server import neo4j_server


def read_yago_dataset(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ')
        for row in reader:
            yield row

expected_data_postgres = {} 
expected_data_mongo = {}   
expected_data_neo4j = {}   

postgres = postgres_server(host="localhost", port=5432, database="nosql_proj", user="shlok", password="shlok")
mongo = mongo_server(host="localhost", port=27017, database="nosql_proj")
neo4j = neo4j_server(uri="bolt://localhost:7687", user="neo4j", password="neo4jpassword")
postgres.connect()
mongo.connect()
neo4j.connect() 

def check_data_consistency (expected_data, server):
    unique_subjects = set(subject for subject, predicate in expected_data.keys())
    for subject in unique_subjects:
        data = server.query(subject)

        for result in data:
            subject, predicate, obj, timestamp = result
            expected_obj, expected_timestamp = expected_data.get((subject, predicate), (None, None))
            
            if obj != expected_obj :
                return 0
    
    return 1

def merge_map (map1, map2) :
    for subject_predicate_pair, (obj2, timestamp2) in map2.items():
        obj1, timestamp1 = map1.get(subject_predicate_pair, (None, None))
        
        if obj1 is not None and timestamp1 is not None:
            if timestamp2 > timestamp1:
                map1[subject_predicate_pair] = (obj2, timestamp2)
            
        elif obj1 is None and timestamp1 is None:
            map1[subject_predicate_pair] = (obj2, timestamp2)


def run_test():
    res = check_data_consistency (expected_data_postgres, postgres)
    if (res == 0) :
        return 0
    res = check_data_consistency (expected_data_mongo, mongo)
    if (res == 0) :
        return 0
    res = check_data_consistency (expected_data_neo4j, neo4j)
    if (res == 0) :
        return 0

    postgres.merge ("mongo")
    mongo.merge ("postgres")

    merge_map (expected_data_postgres, expected_data_mongo)
    merge_map (expected_data_mongo, expected_data_postgres)

    res = check_data_consistency (expected_data_postgres, postgres)
    if (res == 0) :
        return 0
    res = check_data_consistency (expected_data_mongo, mongo)
    if (res == 0) :
        return 0


    mongo.merge ("neo4j")
    neo4j.merge ("mongo")

    merge_map (expected_data_neo4j, expected_data_mongo)
    merge_map (expected_data_mongo, expected_data_neo4j)

    res = check_data_consistency (expected_data_neo4j, neo4j)
    if (res == 0) :
        return 0
    res = check_data_consistency (expected_data_mongo, mongo)
    if (res == 0) :
        return 0


    postgres.merge ("neo4j")
    neo4j.merge ("postgres")

    merge_map (expected_data_neo4j, expected_data_postgres)
    merge_map (expected_data_postgres, expected_data_neo4j)

    res = check_data_consistency (expected_data_neo4j, neo4j)
    if (res == 0) :
        return 0
    res = check_data_consistency (expected_data_postgres, postgres)
    if (res == 0) :
        return 0

    return 1


# Function to perform insertion and testing
def perform_tests(yago_file, insert_interval):
    count = 0
    test_count = 1

    for row in read_yago_dataset(yago_file):
        subject, predicate, obj = row  # Extract subject, predicate, object
        # print (subject, predicate, obj)
        # continue

        if count % 3 == 0:
            postgres.update(subject, predicate, obj)
            expected_data_postgres[(subject, predicate)] = (obj, count)
        elif count % 3 == 1:
            mongo.update(subject, predicate, obj)
            expected_data_mongo[(subject, predicate)] = (obj, count)  
        else :
            neo4j.update (subject, predicate, obj)
            expected_data_neo4j[(subject, predicate)] = (obj, count) 

        count += 1

        # Perform tests after every insert_interval rows
        if count % insert_interval == 0:
            res = run_test ()
            if (res == 1) :
                print (f"Test Case {test_count}: SUCCESS")
            else :   
                print (f"Test Case {test_count}: FAILED")

            test_count += 1


    res = run_test ()
    if (res == 1) :
        print (f"Test Case {test_count}: SUCCESS")
    else :   
        print (f"Test Case {test_count}: FAILED")

    # Close connections
    postgres.disconnect()
    mongo.disconnect()
    neo4j.disconnect()

# Main function
if __name__ == "__main__":
    yago_file = "tests/yago_first_10k.tsv"
    perform_tests(yago_file, 1000)
