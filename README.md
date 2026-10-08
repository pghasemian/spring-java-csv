## Logging

The application uses SLF4J with Spring Boot's default logging implementation.

Logs are provided for important application events, including:

- CSV upload and parsing
- Number of imported records
- Code lookup
- Missing codes
- Duplicate codes
- CSV validation errors
- Deletion of stored records

Example:

```text
INFO  Starting CSV upload
INFO  CSV parsing completed successfully. 18 records parsed
INFO  CSV upload completed successfully. 18 records imported