# big-data-management-analytics
Course projects for Big Data Management and Analytics: MapReduce, Spark, and other large-scale data processing.

# Big Data Management and Analytics

This repository contains coursework and programming assignments for the Big Data Management and Analytics course.

Topics covered include:

- Hadoop MapReduce
- Spark (Apache)
- Distributed data processing
- Docker containerizations
- Large-scale dataset analysis

The goal of this repository is to demonstrate practical experience with distributed computing frameworks used in large-scale data systems.

---

## Repository Structure

assignments/  
Contains implementations of MapReduce programs.

datasets/  
Input datasets used in the experiments.

scripts/  
Helper scripts for compiling and running Hadoop jobs.

docs/  
Documentation on system architecture and workflow.

---

## Example: Running WordCount

Compile the program:

javac -classpath $(hadoop classpath) WordCount.java

Create jar:

jar cf WordCount.jar WordCount*.class

Run the Hadoop job:

hadoop jar WordCount.jar WordCount q1_dataset.txt output

---

## Tools

- Hadoop MapReduce
- Docker
- Intellij IDEA CE (Java, jar files)
- Apache Spark
- Linux

