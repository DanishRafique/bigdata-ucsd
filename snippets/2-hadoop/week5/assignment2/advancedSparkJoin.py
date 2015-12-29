"""
gennum files contain show names and their viewers, genchan files contain
show names and their channel. We want to find out the total number of viewer
across all shows for the channel BAT.

**: We assume the files are on the
 HDFS according to the other exercises.
"""
import sys
from pyspark import SparkConf, SparkContext

def main():
	# Set the configuration of the Spark Application
	conf = (SparkConf().setMaster("local[*]").setAppName("advancedSparkJoin"))

	# Creating a Spark context with the previous configuration
	sc = SparkContext(conf = conf)
	
	# Loading the data
	show_views_file = sc.textFile("input/join2_gennum?.txt")
	show_channel_file = sc.textFile("input/join2_genchan?.txt")

	# Closures to parse the files (using the spark map transformation)
	def split_show_views(line):
		key_value = line.split(",")
		return (key_value[0], int(key_value[1]))

	def split_show_channel(line):
		key_value = line.split(",")
		return (key_value[0], key_value[1])

	# Map
	show_views = show_views_file.map(split_show_views)
	show_channel = show_channel_file.map(split_show_channel)

	# Join
	joined_dataset = show_views.join(show_channel)

	# Extract channel as key
	channel_views = joined_dataset.map(lambda x: (x[1][1], x[1][0]))

	# Sum across (reduce)
	sumChannel = channel_views.reduceByKey(lambda a, b: a + b).collect()
	print sumChannel

if __name__ == "__main__":
	main()