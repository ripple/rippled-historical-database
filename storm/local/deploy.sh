#!/bin/sh
# Run topology locally
mvn compile exec:java -Dstorm.topology=ripple.importer.ImportTopology
