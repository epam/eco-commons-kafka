# Eco Commons Kafka

It's a library of utilities, helpers and higher-level APIs for the [Kafka](https://kafka.apache.org/) client library.

The currently supported version is [2.3.1](https://kafka.apache.org/downloads#2.3.1)

The library can be obtained from the Maven by adding the following dependency in the pom.xml:

```
<dependency>
    <groupId>com.epam.eco</groupId>
    <artifactId>commons-kafka</artifactId>
    <version>${project.version}</version>
</dependency>
```

## Features

* Advanced consumer to handle long-running tasks
* Bootstrap consumer to all records from topic
* Helpers to perform various common tasks (for example count records in topic)
* Transactional Producer to perform atomic consume-process-produce using [transactions](https://www.confluent.io/blog/transactions-apache-kafka/) while preserving possibility to work under automatic partitions rebalance
* In-memory caches
* Common implementations of Serializer/Deserializer 
* [Jackson](https://github.com/FasterXML/jackson) module for serializing/deserializing Kafka types 
* Configuration builders

## Build

```
git clone git@github.com:epam/eco-commons-kafka.git
cd eco-commons-kafka
mvn clean package
```

## Compatibility matrix

Eco Commons Kafka | Kafka
---  | --- 
1.3.x | 2.3.x
1.2.x | 2.2.x
1.1.x | 2.1.x
1.0.x | 2.0.x
0.1.x | 1.0.x

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
