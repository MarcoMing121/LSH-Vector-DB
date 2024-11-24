# LSH Vector Database Controller

A distributed vector database controller based on Locality-Sensitive Hashing (LSH) for efficient similarity search.

## Features

- LSH-based similarity search
- Distributed storage with HBase
- Parallel processing with MapReduce
- REST API interface
- Large file support (up to 2GB)
- Asynchronous task processing

## Prerequisites

- Java 8
- Apache HBase 2.x
- Hadoop 3.3.1
- Maven 3.x
- Spring Boot 2.7.x

## Quick Start

1. Set environment variables:
```bash
export HADOOP_HOME=/usr/local/hadoop-3.3.1
export HBASE_HOME=/usr/local/hbase
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
```

2. Start services:
```bash
# Start Hadoop
start-dfs.sh
start-yarn.sh
mapred --daemon start historyserver

# Start HBase
start-hbase.sh
```

3. Build and run:
```bash
mvn clean compile assembly:single
java -jar target/controller-0.0.1-jar-with-dependencies.jar
```

Or use the provided script:
```bash
./run.sh
```

## API Usage

1. Index documents:
```bash
curl -X POST -F "file=@documents.json" http://localhost:8080/api/documents/index
```

2. Query similar documents:
```bash
curl -X POST -F "file=@query.json" http://localhost:8080/api/documents/query
```

## Configuration

Key settings in application.properties:
```properties
# Server Configuration
server.port=8080

# HBase Configuration
hbase.zookeeper.quorum=localhost
hbase.zookeeper.property.clientPort=2181

# HDFS Configuration
hdfs.defaultFS=hdfs://localhost:9000

# LSH Parameters
lsh.vector.length=100
lsh.rows=2
lsh.bands=5
```

## Project Structure

```
LSH-Vector-DB/
├── controller/
│   ├── src/
│   │   ├── main/
│   │   │   ├── java/
│   │   │   │   └── com/lsh/
│   │   │   │       ├── service/
│   │   │   │       └── controller/
│   │   │   └── resources/
│   │   └── test/
│   └── pom.xml
```

## Core Components

1. **HBaseLoader**: Data ingestion
2. **SignatureCalculator**: LSH signature generation
3. **LSH**: Locality-sensitive hashing implementation
4. **QueryProcessor**: Query handling
5. **WorkflowManager**: Workflow coordination

## License

MIT License