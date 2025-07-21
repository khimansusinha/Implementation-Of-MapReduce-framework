# Implementation-Of-MapReduce-framework
Simulation of the MapReduce framework on a single machine using multi-process programming
# Explanation
In 2004, Google (the paper “MapReduce: Simplified Data Processing on Large Clusters” by J.
Dean and S. Ghemawat) introduced a general programming model for processing and generating
large data sets on a cluster of computers.
The general idea of the MapReduce model is to partition large data sets into multiple splits,
each of which is small enough to be processed on a single machine, called a worker. The data
splits will be processed in two phases: the map phase and the reduce phase. In the map phase, a
worker runs user-defined map functions to parse the input data (i.e., a split of data) into multiple
intermediate key/value pairs, which are saved into intermediate files. In the reduce phase, a
(reduce) worker runs reduce functions that are also provided by the user to merge the intermediate
files, and outputs the result to result file(s).
