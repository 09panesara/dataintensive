package id2221.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import java.util.Iterator;
import static java.lang.Integer.parseInt;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}


	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		// TreeMap sorts on key so stores in increasing size of reputation (higher reputation better)
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		// takes in user record as xml, convert from xml to string map
		Map<String, String> userRecord = transformXmlToMap(value.toString());
		// check Id isn't null
		if (userRecord.get("Id") != null) {
			repToRecordMap.put(parseInt(userRecord.get("Reputation")), new Text(value));
		}

		
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
	    // Output our ten records to the reducers with a null key
		Iterator iterator = repToRecordMap.descendingKeySet().iterator();
		for (int i=0; iterator.hasNext() && i < 10; i++) { // iterate over top ten 
			Text record = repToRecordMap.get(iterator.next());
			context.write(NullWritable.get(), record);
		}
	}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text val: values) { // produce map of reputation to record
			// convert to map to get reputation
			Map<String, String> userRecord = transformXmlToMap(val.toString());
			Integer rep = parseInt(userRecord.get("Reputation"));
			repToRecordMap.put(rep, new Text(val));

		}

		Iterator<Integer> iterator = repToRecordMap.descendingKeySet().iterator();

		// get top 10 records in reducer from repToRecordMap
		for (int i=0; iterator.hasNext() && i < 10; i++) { 
			Integer rep = iterator.next();
			Text record = repToRecordMap.get(rep);
			Map<String, String> userRecord = transformXmlToMap(record.toString());

			// insert to hbase table
			Put insHBase  = new Put(Integer.toString(i).getBytes());
			String id = userRecord.get("Id");

			insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep.toString()));
			insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));

			context.write(NullWritable.get(), insHBase); // write to context
		}

	}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "topten");
		job.setJarByClass(TopTen.class);
		
		job.setMapperClass(TopTenMapper.class);
		job.setReducerClass(TopTenReducer.class);
		
		job.setMapOutputKeyClass(NullWritable.class); // no key needed, just want top 10 records
		job.setMapOutputValueClass(Text.class); // record of type Text

		job.setNumReduceTasks(1); // 1 input group for this reducer containing all the potential top ten records
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// output table
		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
