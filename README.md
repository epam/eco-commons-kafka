# Eco Commons Kafka

It's a library of utilities, helpers and higher-level APIs for the [Kafka](https://kafka.apache.org/) client library.

The currently supported version is [3.0.0](https://kafka.apache.org/downloads#3.0.0)

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
2.0.x | 3.0.x
1.8.x | 2.8.x
1.7.x | 2.7.x
1.6.x | 2.6.x
1.5.x | 2.5.x
1.4.x | 2.4.x
1.3.x | 2.3.x
1.2.x | 2.2.x
1.1.x | 2.1.x
1.0.x | 2.0.x
0.1.x | 1.0.x

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
