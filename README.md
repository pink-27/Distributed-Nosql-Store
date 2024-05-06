
# Distributed NoSQL Triple Store Prototype

This project focuses on the design and implementation of a distributed NoSQL triple store prototype utilizing state-based objects. The system employs multiple servers, each using distinct frameworks (PostgreSQL, MongoDB, and Neo4j) for data processing and storage, while ensuring effective communication and seamless translation of query languages.

## Features

- **Querying**: Retrieve subject-predicate-object triples from the designated server.
- **Updating**: Modify the value (object) associated with a subject-predicate pair.
- **Merging**: Synchronize data across servers through a log-based merging process, ensuring consistency.

## System Architecture

The architecture seamlessly integrates PostgreSQL, MongoDB, and Neo4j to efficiently manage data in a distributed environment. Key aspects include:

- **Data Storage**: PostgreSQL and MongoDB utilize a "triples" table/collection to store subject-predicate-object triples, while Neo4j employs a graph-based data model with nodes representing entities and relationships representing predicates.
- **Log-based Merging**: Each server maintains a log file recording updates to its local state. During merging, servers exchange log files and apply updates based on sequence numbers and timestamps, ensuring proper ordering and conflict resolution.
- **Fault Tolerance**: Sharding log files and implementing a "recover()" method enhance fault tolerance and recovery capabilities.

## Implementation

The project is implemented in Python, leveraging the unique strengths of PostgreSQL, MongoDB, and Neo4j for efficient data storage, retrieval, and querying. Key components include:

- **Data Structures**: Tailored data models for each database system, optimized for storing and querying triples.
- **Querying**: Methods to retrieve data from MongoDB, Neo4j, and PostgreSQL based on specified subjects.
- **Updating**: Methods to modify existing data entries in the respective database systems.
- **Log-based Merging**: A systematic process for exchanging and merging data between servers, ensuring consistency across the distributed system.

## Testing

The `yago_test.py` script simulates data updates and validates consistency across the three database servers using the YAGO dataset, a massive collection of facts. Test cases include:

- Data insertion verification
- Merge validation, ensuring correct conflict resolution based on timestamps

## User Interface

The project features a user-friendly command-line interface, allowing users to interact with the PostgreSQL, Neo4j, and MongoDB databases. Users can perform various tasks, such as querying, updating, and merging data between servers, through simple commands and input prompts.

## Evaluation and Results

The prototype's functionality was rigorously tested using `basic.py` and `yago_test.py` scripts, demonstrating successful data insertion, updating, and merging operations across varying dataset sizes. While scalability challenges remain, the prototype closely aligns with the project's objectives of enabling efficient interaction with PostgreSQL and MongoDB databases.

## Future Improvements

Potential areas for future enhancement include:

- Implementing advanced scaling solutions to efficiently manage and merge data across a large number of servers.
- Addressing log file durability concerns to ensure data integrity.
- Incorporating more precise time-tracking mechanisms for improved data accuracy.

