package ouyang;

import ouyang.PageRank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class AddInfo extends Configured {//implements Tool {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		static enum Counters {INPUT_WORDS}

		private Text outputKey = new Text();
		private Text outputValue = new Text();
		private Text thisLink = new Text();
		private double rank = 0.0;
		private int degree = 0;

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
				
			String line = value.toString();
			String[] parts = line.split("[ \t]");

			degree = parts.length - 1;

			StringBuilder builder = new StringBuilder();
			if (parts.length >= 3) {
				for (int i = 2; i < parts.length; i++) {
					builder.append(parts[i] + " ");
				}
			}
			outputKey.set("Info:");
			outputValue.set(String.valueOf(degree));
			output.collect(outputKey, outputValue);
                        
                        
			//outputValue.set(parts[1] + " " + builder.toString());
			//output.collect(thisLink, outputValue);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		//private double factor = 0.85;
		private Text outputValue = new Text();
		private int numNodes = 0;
		private int numEdges = 0;
		private int minDegree = 99999;
		private int maxDegree = 0;

		// public void configure(JobConf job) {
		// job.getDouble("pagerank.factor",0.85);
		// }

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			//double sum = 0.00;
			//String outlinks = "";

			while (values.hasNext()) {// && key.toString().equals("Info:"))
				String line = values.next().toString();
				String[] parts = line.split("[ \t]");
				numNodes++;
				
				int degree = Integer.parseInt(parts[0]);
				numEdges += degree;
				if(degree<minDegree)
				        minDegree = degree;
				if(degree > maxDegree)
				        maxDegree = degree;	
			}
                        if(key.toString().equals("Info:")){
                        Text out = new Text();
                        out.set("\nNumber of Node: "+numNodes+ "\nNumber of Edges: "+ numEdges + "\nAverage Degree: " + numEdges/numNodes + "\nMax Degree: "+ maxDegree + "\nMin Degree: "+ minDegree);
                        output.collect(key, out);
                        } else {
			output.collect(key, values.next());
			}
		}
	}
}
