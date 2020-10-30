## Drug Interaction Prediction Using Spark and Graph Link Prediction Algorithms 

This project uses Spark and GraphML techniques to predict drug interactions. 

#### Background
A large number of drugs are introduced every year, and it is common to take several different medications at the same time, especially in the elderly/chronically ill. The administration of a combination of drugs can cause a drug interaction, which is when the effect of one drug is modified by another drug and can cause serious, unexpected side-effects for patients. For this reason, studying drug interactions and facilitating their prediction with the incorporation of new drugs is an important problem to address.

#### Methodology
We approached this problem using a graph approach. Predicting drug interactions is particularly suited to graph data since each drug can be modelled as a node, and each edge/link as an (unwanted) drug interaction. The task of predicting drug interactions then becomes link pre- diction in the domain of graphs. In this project, we applied Node2Vec and Graph Convolutional Networks (GCNs) for link prediction on the DrugBank database. Feature extraction was carried out using Spark.

#### Running the code
1. From inside this directory, run: ` docker run --rm -p 10000:8888 -e JUPYTER_ENABLE_LAB=yes -v "$PWD":/home/jovyan/work jupyter/all-spark-notebook`
2. Open localhost:10000 in your browser and run notebook with spylon-kernel for spark with scala, or with the python3 kernel for pyspark.
