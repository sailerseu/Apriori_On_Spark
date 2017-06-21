Our work is on frequent itemset mining for big datasets.

There are two folders here, Spark, Hadoop and result. Spark and Hadoop contain the source code corresponded. 

The result shows the result of the algorithms.
 
command lines are as follows:

spark-submit --class frequent_items_finding.SApriori_On_Item_K*   --master yarn-cluster  XXX.jar   input  iteration   Minsup

hadoop jar XXX.jar   input  output iteration   Minsup