package ouyang;

import ouyang.SortRank;
import ouyang.AddInfo;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank extends Configured implements Tool {

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

			thisLink.set(parts[0]);
			if (parts.length >= 2) {
				if (parts[1].length() < 10) { // A rank b c...
					rank = Double.valueOf(parts[1]);
					degree = parts.length - 2;

					StringBuilder builder = new StringBuilder();
					if (parts.length >= 3) {
						for (int i = 2; i < parts.length; i++) {
							builder.append(parts[i] + " ");
						}
					}
					outputValue.set("info " + String.valueOf(degree) + " "
							+ builder.toString());
					output.collect(thisLink, outputValue);

					outputValue.set(thisLink.toString() + " "
							+ String.valueOf(rank) + " "
							+ String.valueOf(degree));
					if (parts.length >= 3) {
						for (int i = 2; i < parts.length; i++) {
							outputKey.set(parts[i]);
							output.collect(outputKey, outputValue);
							reporter.incrCounter(Counters.INPUT_WORDS, 1);
						}
					}
				} else { // A (no rank) b c ...
					rank = 1.0;
					degree = parts.length - 1;

					StringBuilder builder = new StringBuilder();
					if (parts.length >= 2) {
						for (int i = 1; i < parts.length; i++) {
							builder.append(parts[i] + " ");
						}
					}
					outputValue.set("info " + String.valueOf(degree) + " "
							+ builder.toString());
					output.collect(thisLink, outputValue);

					outputValue.set(thisLink.toString() + " "
							+ String.valueOf(rank) + " "
							+ String.valueOf(degree));
					for (int i = 1; i < parts.length; i++) {
						outputKey.set(parts[i]);
						output.collect(outputKey, outputValue);
						reporter.incrCounter(Counters.INPUT_WORDS, 1);
					}
				}
			} else {// only A
				rank = 1.0;
				degree = parts.length - 1;

				outputValue.set("info 0");
				output.collect(thisLink, outputValue);
			}

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private double factor = 0.85;
		private Text outputValue = new Text();
		//private int numNodes = 0;
		//private int numEdges = 0;
		//private int minDegree = 1000;
		//private int maxDegree = 1;

		// public void configure(JobConf job) {
		// job.getDouble("pagerank.factor",0.85);
		// }

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

			double sum = 0.00;
			String outlinks = "";

			while (values.hasNext()) {
				String line = values.next().toString();
				String[] parts = line.split("[ \t]");

				if (!parts[0].equals("info")) {//Not info message
					sum += Double.valueOf(parts[1]) / Double.valueOf(parts[2]);
				} else {// info message
					// numNodes++;
					// int degree = Integer.parseInt(parts[1]);
					// numEdges += degree;
					// minDegree = degree < minDegree ? degree : minDegree;
					// maxDegree = degree > maxDegree ? degree : maxDegree;
					if (parts.length > 2) {
						for (int i = 2; i < parts.length; i++) {
							outlinks = outlinks.concat(parts[i] + " ");
						}
					}
				}
			}
			sum = factor * sum + 1 - factor;
			sum = Math.round(sum * 10000.0) / 10000.0;
			outputValue.set(String.valueOf(sum) + " " + outlinks);
			output.collect(key, outputValue);
		}
	}
	
	
	

        
	public int run(String[] args) throws Exception {
                
                JobConf conf = new JobConf(PageRank.class);
                FileSystem fs = FileSystem.get(conf);
                //Path temp = new Path("temp");
                Path inpath = new Path(" ");
                Path outpath = new Path(" ");
                int numIter = (args.length == 3) ? Integer.parseInt(args[2]) : 16;
                
                for(int i = 1; i<=numIter;i++){
                        inpath = (i == 1) ? (new Path(args[0])) : (new Path("outputOfThe"+String.valueOf(i-1)));
                        //outpath = (i == numIter) ? (new Path(args[1])) : (new Path("outputOfThe"+String.valueOf(i)));
                        outpath = new Path("outputOfThe"+String.valueOf(i));
                        conf = new JobConf(PageRank.class);
                  
                        conf.setJobName("PageRankIteration");
		        conf.setMapperClass(Map.class);
		        conf.setReducerClass(Reduce.class);
		        conf.setOutputKeyClass(Text.class);
		        conf.setOutputValueClass(Text.class);
		        conf.setInputFormat(TextInputFormat.class);
		        conf.setOutputFormat(TextOutputFormat.class);
		        FileInputFormat.setInputPaths(conf, inpath);
	        	FileOutputFormat.setOutputPath(conf, outpath);
	        	JobClient.runJob(conf);
	        	fs = FileSystem.get(conf);
	        	if(i!=1) {
	        	        fs.delete(new Path("outputOfThe"+String.valueOf(i-1)),true);
	        	}
                }
                
                conf = new JobConf(SortRank.class);
                conf.setJobName("SortRank");
		conf.setMapperClass(SortRank.Map.class);
		conf.setReducerClass(SortRank.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("outputOfThe"+String.valueOf(numIter)));
	        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	        JobClient.runJob(conf);
	        fs = FileSystem.get(conf);
	        fs.delete(new Path("outputOfThe"+String.valueOf(numIter)),true);
	        
	        conf = new JobConf(AddInfo.class);
                conf.setJobName("AddInfo");
		conf.setMapperClass(AddInfo.Map.class);
		conf.setReducerClass(AddInfo.Reduce.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
	        FileOutputFormat.setOutputPath(conf, new Path(args[1] + "GraphInfo"));
	        JobClient.runJob(conf);
	        //fs = FileSystem.get(conf);
	        //fs.delete(new Path("outputOfThe"+String.valueOf(numIter)),true);
                
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PageRank(), args);
		System.exit(res);
	}

}
