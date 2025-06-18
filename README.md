# Cantrip

Cantrip is both (1) a Python protocol for semantic layers, and (2) a reference implementation that infers semantics from the database schema.

As a protocol, Cantrip provides a way to integrate applications like Apache Superset with different semantic layers (DJ, MetricFlow, Snowflake, etc.) via a common interface. All is needed is an implementation of the Cantrip protocol, and the application can use it to query the semantic layer.

As a reference implementation, Cantrip offers a simple serverless semantic layer that can be quickly deployed using SQLite, Postgres, Trino, or any other SQL database.
