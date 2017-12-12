# Rdb - connector - test

[![](https://jitpack.io/v/emartech/rdb-connector-test.svg)](https://jitpack.io/#emartech/rdb-connector-test)

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

## Tasks:

Contains general test implementations, that may be used in connectors to test connector specific behavior. When applied on a given connector, all the test assertions should be fulfilled assuming correct implementation.

## Dependencies:

**[Rdb - connector - common](https://github.com/emartech/rdb-connector-common)** - defines a Connector trait, that should be implemented by different types of connectors. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)

