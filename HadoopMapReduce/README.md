## A README file explaining how to run the JAR files:

We run the JAR files which we created using IntelliJ IDEA CE using Docker, where we use only 2 containers mainly namenode and resourcemanager in the following manner:

# Step 1 
- We start the containers using the command: docker compose up -d, and check if they are running (and healthy to use) by using the command: docker compose ps

After this step, the Hadoop cluster is up and running.

---

# Step 2 
- We now copy the input dataset (our words.txt file for example) from host to the Docker container namenode using the command: docker cp words.txt namenode:/tmp/words.txt 

# Step 3 
- We now create an input directory in HDFS using the command: docker exec -it namenode hdfs dfs -mkdir -p /input

# Step 4 
- We then copy the file from the container to this input directory using the command: docker exec -it namenode hdfs dfs -put -f /tmp/words.txt /input/words.txt, and we check if the transfer is successful by seeing the contents of the directory using the command: docker exec -it namenode hdfs dfs -ls /input

After this step, the input file is now ready to be run in the container. All we need to do is to upload the JAR files in the resourcemanager container and run it.

---
# Step 5 
- We transfer the JAR file (for example, WordCount.jar) from the host into the tmp directory of the resourcemanager container using the command: docker cp WordCount.jar resourcemanager:/tmp/

After this step, the program file is ready for execution/compiling on our .txt file.

# Step 6 
- We run the Hadoop clusters on WordCount application using the command: Docker exec -it resourcemanager Hadoop jar /tmp/WordCount.jar /input/words.txt /output

After this, we will try to view and download the output file.

---
# Step 7 
- To see the list of output files in our namenode container, we use the command: docker exec -it namenode hdfs dfs -ls /output, and to see the contents of the output file in terminal/command window, we run the command: docker exec -it namenode hdfs dfs -cat /output/part-r-00000

# Step 8 
- To download the output file, we first copy it from HDFS to the namenode container using the command: docker exec -it namenode hdfs dfs -get /output/part-r-00000 /tmp/part-r-00000, and then we copy from the namenode container to the host using the command: docker cp namenode:/tmp/part-r-00000 ./part-r-00000

After this step, we have created the output file on our host, which we can open anytime.

---
# Step 9 
- We remove the existing output directory in HDFS before re-running a job with the same output path using the command: docker exec -it namenode hdfs dfs -rm -r -f /output

# Step 10 
- We stop and clean up the Hadoop cluster, and remove the HDFS data by running the command: docker compose down --volumes

We add --volumes to remove the HDFS data also, otherwise we only free the CPU and remove the cluster files. To simply stop the cluster and free up the memory, we can run the command: docker compose stop

---
