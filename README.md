# KmeansMapReduce
A simple Kmeans algorithm using Map/Ruduce for Hadoop

#### To compile and create jar (using maven):
 - mvn package

#### To run
 - yarn jar target/kmeans-0.0.1.jar input.csv output.csv #clusters Indexes_of_columns_to_use

*For example*
 - yarn jar target/kmeans-0.0.1.jar data.csv output.csv 3 4 5 6

Warning if one of the row in the input file has a missing data this will not work.
