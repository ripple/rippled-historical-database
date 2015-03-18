#!/bin/sh

mvn clean compile
mvn package
storm jar target/importer-0.0.1-jar-with-dependencies.jar ripple.importer.ImportTopology "ripple-ledger-importer"
