# CSV Code Service

A Java Spring Boot application that uploads and validates CSV files, publishes records to Apache Kafka, and asynchronously persists them in an in-memory H2 database. The application provides REST APIs to retrieve and delete stored records.

## Features

- Upload CSV files through a REST API.
- Validate CSV headers, required fields, dates, and unique codes.
- Publish validated records to an Apache Kafka topic.
- Process Kafka messages using a consumer group.
- Persist records in an in-memory H2 database.
- Retrieve all records or find a record by its code.
- Delete all stored records.
- Log important application events.

## Technologies

- Java 21
- Spring Boot 4.1.1
- Maven
- Spring Web MVC
- Spring Data JPA
- H2 Database
- Apache Kafka 4.0.1
- Docker Compose
- Apache Commons CSV
- Jackson
- JUnit 5 and Mockito

## Architecture

The application processes uploaded data through the following flow:

CSV Upload → CSV Validation → Kafka Producer → Kafka Cluster → Kafka Consumer → H2 Database → REST API

Kafka runs as a three-broker cluster with three partitions and a replication factor of three.

The application publishes records to the `codes` topic. The consumer group `code-consumer-group` processes the messages and saves the records to the database.

**Note:** Kafka processing is asynchronous. Records may not be immediately available through the retrieval endpoints after an upload request.

## CSV Format

The CSV file must contain the following headers in the specified order:

```csv
source,codeListCode,code,displayValue,longDescription,fromDate,toDate,sortingPriority
```

Example:

```csv
source,codeListCode,code,displayValue,longDescription,fromDate,toDate,sortingPriority
ZIB,ZIB003,415882003,Axillaire temperatuur,,01-01-2019,,
```

### Validation Rules

- The first line must contain the expected headers in the correct order.
- Required fields must not be empty.
- The `code` field must be unique within the uploaded file and must not already exist in the database.
- Dates must use the `dd-MM-yyyy` format.
- `longDescription` and `toDate` may be empty.
- `sortingPriority` may be empty; otherwise, it must be a valid integer.
- Empty CSV files and files without data records are rejected.

## Prerequisites

Install the following software:

- JDK 21
- Docker Desktop with Docker Compose
- Git (optional, for cloning the repository)

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/pghasemian/spring-java-csv.git
cd spring-java-csv
```

### 2. Start Kafka

Start the three-broker Kafka cluster from the project root:

```bash
docker compose up -d
```

Verify that the brokers are running:

```bash
docker compose ps
```

### 3. Create the Kafka Topic

The Docker Compose configuration disables automatic topic creation. Create the `codes` topic with three partitions and a replication factor of three:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --topic codes --partitions 3 --replication-factor 3
```

If the topic already exists, it does not need to be created again.

### 4. Run the Application

On Windows, execute:

```powershell
.\mvnw.cmd spring-boot:run
```

The application runs on:

```text
http://localhost:8080
```

### 5. Run the Tests

On Windows, execute:

```powershell
.\mvnw.cmd test
```

## REST API

Base URL:

```text
http://localhost:8080/api/codes
```

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/codes/upload` | Upload a CSV file and publish its records to Kafka |
| GET | `/api/codes` | Retrieve all stored records |
| GET | `/api/codes/{code}` | Retrieve a record by its unique code |
| DELETE | `/api/codes` | Delete all stored records |

### Upload CSV

**Request**

```http
POST /api/codes/upload
Content-Type: multipart/form-data
```

Use a form-data field named `file` and select the CSV file.

**Successful response — HTTP 201 Created**

```json
{
  "message": "CSV published to Kafka successfully",
  "recordsPublished": 18
}
```

This response indicates that the records have been submitted for publication to Kafka. It does not guarantee that every record has already been persisted in H2.

### Retrieve All Records

```http
GET /api/codes
```

Returns an array of stored records.

### Retrieve a Record by Code

```http
GET /api/codes/415882003
```

Returns the matching record when it exists. If the code does not exist, the API returns HTTP `404 Not Found`.

### Delete All Records

```http
DELETE /api/codes
```

Returns HTTP `204 No Content` when the delete operation succeeds.
**Note:** The delete operation removes records from the H2 database but does not delete pending messages from Kafka. If unprocessed messages remain, records may be inserted into the database again after the delete request.

## H2 Database Console

The application uses an in-memory H2 database.

Console URL:

```text
http://localhost:8080/h2-console
```

Connection settings:

| Setting | Value |
|---|---|
| JDBC URL | `jdbc:h2:mem:codesdb` |
| User Name | `sa` |
| Password | Leave empty |

The database is recreated when the application restarts because the schema uses `create-drop`.

## Logging

The application uses SLF4J with Spring Boot's default logging implementation.

Logs cover important events, including:

- CSV upload and parsing
- CSV validation
- Kafka message publication
- Kafka message consumption and database persistence
- Code lookup
- Missing and duplicate codes
- Deletion of stored records

Example:

```text
INFO  Starting CSV upload
INFO  CSV parsing completed successfully. 18 records parsed
INFO  Code sent to Kafka. code=415882003, partition=0
INFO  Code consumed and saved to database. code=415882003
```

## Project Structure

```text
src
├── main
│   ├── java/com/example/csvcodeservice
│   │   ├── config
│   │   ├── controller
│   │   ├── dto
│   │   ├── entity
│   │   ├── exception
│   │   ├── repository
│   │   └── service
│   └── resources
│       └── application.properties
└── test
    └── java/com/example/csvcodeservice
```

The project root also contains `pom.xml`, `mvnw`, `mvnw.cmd`, and `docker-compose.yml`.

## Stopping Kafka

To stop the Kafka cluster:

```bash
docker compose down
```

Kafka data is stored in Docker volumes defined in the Compose configuration.
