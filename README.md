hadoop-page-rank
================

Iterative MapReduce program to implement Page Rank using Amazon EC2 and FutureGrid to achieve the following objective given sample graphs(each line starts with the node id and then its adjacency list):

Task 1: For each of the sample graphs (small, medium, large), list the top ten nodes in order of descending PageRank value. In addition, list a summary of each graph property, e.g., number of nodes, number of edges, and (min, max, avg) of out-degree for each node; list the total number of MapReduce iterations for convergence. 

Task 2: Run the same program on both FutureGrid and AWS and compare their performance, e.g. total execution time, execution times for the first 10 iterations.
