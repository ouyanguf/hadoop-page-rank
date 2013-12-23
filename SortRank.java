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

public class SortRank extends Configured {//implements Tool {

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text();
		private Text outputValue = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
		        throws IOException {
				
			String line = value.toString();
			String[] parts = line.split("[ \t]");
			outputValue.set(parts[0]);
                        outputKey.set(String.valueOf(100.0 - Double.valueOf(parts[1])));
			output.collect(outputKey, outputValue);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

                private int numOutput = 0;
                //private Text empty = new Text();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			while (values.hasNext()) {
			//String line = values.next().toString();
			double thekey = Math.round((100.0 - Double.valueOf(key.toString())) * 10000.0) / 10000.0;
			Text newkey = new Text(String.valueOf(thekey));
			//value.set(line);
			//System.out.println(numOutput);
                        if(numOutput < 10) { //Number of displayed records
			output.collect(newkey, values.next());
			numOutput++;
			} else {
			values.next();
			//output.collect(empty, empty);
			}
			
		}
		}
	}
}
