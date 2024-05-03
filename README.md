# Vectorfriends Neo4j Ingest
Cloud function for importing form data from [Vectorfriends](https://github.com/itsajchan/vectorfriends)


## Running Locally
```
NEO4J_URI=<uri> \
NEO4J_USER=<username> \
NEO4J_PASSWORD=<password> \
BASIC_AUTH_USER=user \
BASIC_AUTH_PASSWORD=password \
OPENAI_API_KEY=<openai_api_key> \
poetry run functions-framework --target=import_form
```

Default port is 8080
To adjust add `--port=<port_number>` to the above