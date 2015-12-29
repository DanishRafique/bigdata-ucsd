"""
In this programming assignment we will implement in Spark the same
code in the programming assignment in module 4 lesson 2 to perform a
join of 2 different wordcount datasets.

**: We assume the join1_FileA and join1_FileB are on the
 HDFS according to the other exercises.
"""
import sys
from pyspark import SparkConf, SparkContext

def main():
	# Set the configuration of the Spark Application
	conf = (SparkConf().setMaster("local[*]").setAppName("sparkJoin"))

	# Creating a Spark context with the previous configuration
	sc = SparkContext(conf = conf)
	
	# Loading the data
	fileA = sc.textFile("input/join1_FileA.txt")
	fileB = sc.textFile("input/join1_FileB.txt")

	# Closures for split the files (using the spark map transformation)
	def split_fileA(line):
		key_value = line.split(",")
		return(key_value[0], int(key_value[1]))

	def split_fileB(line):
		tmp = line.split(",")
		key_value = tmp[0].split(" ")
		return(key_value[1], key_value[0] + " " + tmp[1])

	# Map
	splited_fileA = fileA.map(split_fileA)
	splited_fileB = fileB.map(split_fileB)

	# Execute the Join
	joined_files = splited_fileB.join(splited_fileA)
	print joined_files.collect()




if __name__ == "__main__":
	main()