## EVA (Exploratory Video Analytics)

[![Build Status](https://travis-ci.org/georgia-tech-db/Eva.svg?branch=master)](https://travis-ci.com/georgia-tech-db/Eva)
[![Coverage Status](https://coveralls.io/repos/github/georgia-tech-db/Eva/badge.svg?branch=master)](https://coveralls.io/github/georgia-tech-db/Eva?branch=master)
### Table of Contents
* Installation
* Demos
* Eva core
* Eva storage 
* Dataset 

### Current Updates
11/3/2019
Integrated the first verison of the Rule Based Query Optimizer
To test run Eva/evaQL/build/testRuleBasedOptimizer.py

Currently, this only completes the Projection Pushdown rule when the following node is a SelectNode since Joins have not been implemented in the current verision of the logical plan tree.

### Installation
* Clone the repo
* Create a virtual environment with conda (explained in detail in the next subsection)


##### How to create the virtual environment
* Install conda - we have prepared a yaml file that you can directly use with conda to install a virtual environment 
* Navigate to the eva repository in your local computer
* conda env create -f environment.yml
* Note, this yaml file should install and all code should run with no errors in Ubuntu 16.04.
   However, there are know installation issues with MacOS.
    
### Demos
We have demos for the following components:
1. Eva analytics (pipeline for loading the dataset, training the filters, and outputting the optimal plan)
```commandline
   cd <YOUR_EVA_DIRECTORY>
   python pipeline.py
```
2. Eva Query Optimizer (Will show converted queries for the original queries)
```commandline
   cd <YOUR_EVA_DIRECTORY>
   python query_optimizer/query_optimizer.py
```
3. Eva Loader (Loads UA-DETRAC dataset)
```commandline
   cd <YOUR_EVA_DIRECTORY>
   python loaders/load.py
```

NEW!!! There are new versions of the loaders and filters.
```commandline
   cd <YOUR_EVA_DIRECTORY>
   python loaders/uadetrac_loader.py
   python filters/minimum_filter.py
```

2. EVA storage-system (Video compression and indexing system - *currently in progress*)

### Eva Core
Eva core is consisted of
* Query Optimizer
* Filters
* UDFs
* Loaders

##### Query Optimizer
The query optimizer converts a given query to the optimal form. 

All code related to this module is in */query_optimizer*

##### Filters
The filters does preliminary filtering to video frames using cheap machine learning models.
The filters module also outputs statistics such as reduction rate and cost that is used by Query Optimizer module.

The preprocessing method below is running:
* PCA

The filters below are running:
* KDE
* DNN
* Random Forest
* SVM

All code related to this module is in */filters*

##### UDFs
This module contains all imported deep learning models. Currently, there is no code that performs this task. It is a work in progress.
Information of current work is explained in detail [here](src/udfs/README.md).

All related code should be inside */udfs*

##### Loaders
The loaders load the dataset with the attributes specified in the *Accelerating Machine Learning Inference with Probabilistic Predicates* by Yao et al.

All code related to this module is in */loaders*

### Eva storage
Currently a work in progress. Come check back later!


### Dataset
__[Dataset info](data/README.md)__ explains detailed information about the  datasets




