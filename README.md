
<h1 align="center">Parallel K-Means using Hadoop</h1>
<div align="center">
    <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Hadoop_logo.svg/1920px-Hadoop_logo.svg.png?20130221043911" />
</div>


## Objective
This repository contains an implementation of the Parallel K-Means clustering algorithm using the Apache Hadoop framework. The objective of this project is to enhance understanding of Apache Hadoop and gain experience with the MapReduce programming model for large-scale data processing.

## Description
Clustering is a common task in data analysis where a set of unlabeled data is grouped such that elements within each group are more similar to each other than they are to elements in other groups. This project focuses on the K-Means clustering algorithm, which falls under the field of unsupervised learning. 

With the growth of data volumes in modern applications, efficient parallel clustering algorithms become essential to meet scalability and performance requirements. The Hadoop and MapReduce framework provide a convenient platform for parallelizing computations across large-scale clusters.

## Specifications
- The repository contains an implementation of the Parallel K-Means algorithm using the MapReduce framework.
- The algorithm is evaluated using the IRIS dataset for comparison with the original K-Means algorithm in terms of runtime and clustering accuracy.
- The implementation is generalized to accept different sizes of feature vectors.

## Contents
- `src/main/java/org/example/`: Contains Java source code for the MapReduce implementation of K-Means clustering.
  - `KMeansMapper.java`: Mapper class for the MapReduce job.
  - `KMeansReducer.java`: Reducer class for the MapReduce job.
  - `PointWritable.java`: Custom Writable class for representing data points.
  - `KMeansDriver.java`: Main driver class to run the K-Means clustering job.
- `README.md`: This file providing an overview of the repository and its contents.
- `LICENSE`: License information for the repository.


## Usage
To run the Parallel K-Means algorithm:
1. Compile the Java source files and create the executable JAR.
2. Ensure Hadoop is properly configured and running.
3. Execute the `KMeansDriver` class with appropriate input parameters.

Example usage: From the root directory of the project execute:
```bash
$ mvn clean install
$ java -jar target/K-Means-Hadoop-1.0.jar <input_dir> <output_dir> <number_of_classes>
```

After running `mvn clean install`, the executable JAR file will be generated in the `target` directory, and you can use the `java -jar` command to execute it with the specified input parameters.

Replace `<input_dir>` with the directory containing input data, `<output_dir>` with the desired output directory, and `<number_of_classes>` with the desired number of clusters.

## License
This project is licensed under the [MIT License](LICENSE).

---
