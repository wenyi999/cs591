# This repository contains solutions to the CS591 L1 assignments 

## Pre-requisites for running queries:

1. Python (3.7+)
2. [Pytest](https://docs.pytest.org/en/stable/)
3. [Ray](https://ray.io)

## Assignments requirements

The detailed assignments requirements can be found in the assignments folder. 

The first assignment let us to implement operators like Scan, Project, Filter, Join, AVG, GroupBy, OrderBy, Limit and Hist. Then we are required to combine them to do 3 queries and to change these queries into Ray actors.

The second assignment let us to extend the operators we built in Assignment 1 with support for backward tracing, Where-provenance query and How-provenance query. Also we need to implement a method that returns the list of all tuples with responsibility ( float ) ρ ≥ 0.5. 

The third assignment is focused on the explanation of machine learning results. Thus, we used a different dataset. We are required to compute some basic statistics about the dataset using the previous operator library. Second, we used the operator library to perform some ETL (Extract-Transform-Load) operations that are necessary in order to train the model. Third, we trained a tree-based model using the Light Gradient Boosting Machine framework on the preprocessed data. Finally, we used two interpretability techniques ( LIME and SHAP ) from the InterpretML toolkit to help users understand what the model has learned and how it makes its predictions.

The fourth assignment focuses on performance analysis for distributed data processing applications. We used OpenTracing to trace the operator library and the backward tracing by instrumenting the operator library and the backward tracing using Jaeger. Then we deploy the distributed system on MOC(Massachusetts Open Cloud) and redo the performance analysis.

## Input Data

Queries of assignments 1 and 2 expect two space-delimited text files (similar to CSV files). 

The first file (friends) must include records of the form:

|UID1 (int)|UID2 (int)|
|----|----|
|1   |2342|
|231 |3   |
|... |... |

The second file (ratings) must include records of the form:

|UID (int)|MID (int)|RATING (int)|
|---|---|------|
|1  |10 |4     |
|231|54 |2     |
|...|...|...   |

## Running queries of Assignment 1

You can run queries as shown below: 

```bash
$ python assignment_12.py --task [task_number] --friends [path_to_friends_file.txt] --ratings [path_to_ratings_file.txt] --uid [user_id] --mid [movie_id]
```

For example, the following command runs the 'likeness prediction' query of the first task for user id 10 and movie id 3:

```bash
$ python assignment_12.py --task 1 --friends friends.txt --ratings ratings.txt --uid 10 --mid 3
```

The 'recommendation' query of the second task does not require a movie id. If you provide a `--mid` argument, it will be simply ignored.

## Running queries of Assignment 2

TODO
