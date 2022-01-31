# SWE5003 NoSQL and Spark Query Language workshop

This is a Scala implementation of the NoSQL and Spark Query Language workshop

The workshop was given as part of the Big Data Engineering for Analytics module which fulfills a requirement for the Engineering Big Data certificate issued by [NUS-ISS](https://www.iss.nus.edu.sg/)

## Getting started
You have 2 options to run the spark job
1. Compile and run on a spark-cluster
2. Use Intellij

### Compile and run on a spark-cluster
Do this if you have a spark cluster to spark-submit to
Use [sbt]((https://www.scala-sbt.org/)) to compile into a jar
```
sbt compile
```
The jar file will be in target/scala-2.12

Use spark-submit to submit the spark job
```
spark-submit {your-jar-file}
```

### Use Intellij
Install Intellij and use it to Open the build.sbt file as a Project

Intellij will resolve the dependencies listed in build.sbt

Go to Run > Edit Configurations > Modify options > Add depenencies with "provided" scope to classpath

Run > Run Main


## Data

The data was oringally provided by [rvenkatiss](https://github.com/rvenkatiss/BEAD_DATA)

## Scala setup

I used [sbt 1.6.1](https://www.scala-sbt.org/) to setup the project

I used the [spark-sbt.g8 from MrPowers](https://github.com/MrPowers/spark-sbt.g8)


## Docker

This project uses a MySQL server to simulated data extraction from an RDBMS

I used Docker to setup this up
```
docker run --name norman-mysql -e MYSQL_ROOT_PASSWORD=norman -p 3306:3306 -d mysql:5.7.37
```