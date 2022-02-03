# SWE5003 NoSQL and Spark Query Language workshop

This is a Scala implementation of the NoSQL and Spark Query Language workshop

The workshop was given as part of the Big Data Engineering for Analytics module which fulfills a requirement for the Engineering Big Data certificate issued by [NUS-ISS](https://www.iss.nus.edu.sg/)

## Getting started

### Clone the repo
```
https://github.com/frenoid/tour-of-spark.git
```

### Running the Spark job
You have 2 options to run the spark job
1. Compile and run on a spark-cluster
2. Use Intellij (Recommended)

### (Option 1) Compile and run on a spark-cluster
Do this if you have a spark cluster to spark-submit to <br />
Take note of these versions. See also [build.sbt](./build.sbt)
```
scala = 2.12.10
spark = 3.0.3
sbt = 1.6.1
```
Use [sbt]((https://www.scala-sbt.org/)) to compile into a jar
```
sbt compile
```
The jar file will be in target/scala-2.12

Use spark-submit to submit the spark job
```
spark-submit {your-jar-file}
```

### (Option 2 RECOMMENDED) Use Intellij
Install Intellij and use it to Open the build.sbt file as a Project

Intellij will resolve the dependencies listed in build.sbt

Go to Run > Edit Configurations > Modify options > Add dependencies with "provided" scope to classpath

Run > Run Main

## Data

### BEAD_DATA
The data was provided by [rvenkatiss](https://github.com/rvenkatiss/BEAD_DATA)

### Set up MYSQL Database
This project uses a MySQL server to simulated data extraction from an RDBMS

I used Docker to setup this up
```
docker run --name norman-mysql -e MYSQL_ROOT_PASSWORD=norman -p 3306:3306 -d mysql:5.7.37
```
Enter the docker container
```
docker exec -ti {container_id} bash
```
Open a MySQL shell and enter the password of "norman" when prompted
```
mysql -u root -p
```
Copy and paste the contents of [Movies.SQL](src/main/resources/Movies.SQL) into the shell<br />
Exit the shell
```
exit
```

### Set up MySQL JDBC driver
For the Spark job to read from MySQL, it needs the MySQL JDBC driver <br />
1. Download it from [Maven](https://mvnrepository.com/artifact/mysql/mysql-connector-java/5.1.49) <br />
2. Then add it to Intellij's dependencies list: File > Project Structure > Dependencies > + and pick the JDBC driver file

## Giter8 template
I used  [spark-sbt.g8 from MrPowers](https://github.com/MrPowers/spark-sbt.g8)