package hw1.mapreduce.code.part1;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class MaxAgeGroup  {


	public static class UserDetailsMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException  {
			String arr[] = value.toString().split(",");
			if(arr.length == 10){
				Long currentUserId = Long.parseLong(arr[0]);
				String[] birthday = arr[9].toString().split("/");

				Date now = new Date();
				int nowMonth = now.getMonth()+1;
				int nowYear = now.getYear()+1900;
				int result = nowYear - Integer.parseInt(birthday[2]);

				if (Integer.parseInt(birthday[0]) > nowMonth) 
					result--;
				else if (Integer.parseInt(birthday[0]) == nowMonth) {
					int nowDay = now.getDate();

					if (Integer.parseInt(birthday[1]) > nowDay) 
						result--;
				}

				String outData = arr[1]+","+new Integer(result).toString()+","+arr[3]+","+arr[4]+","+arr[5];
				context.write(new LongWritable(currentUserId), new Text(("B: " + outData)));
			}
		}

	}


	public static class FriendListMapper extends Mapper<LongWritable, Text, LongWritable, Text>  {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("\t");
			Long userId = Long.parseLong(line[0]);
			if (line.length != 1) {
				String friends = line[1].toString();
				context.write(new LongWritable(userId), new Text("F: " + friends.toString()));
			}
		}
	}


	public static class AgeMapper extends Mapper<LongWritable, Text, AgeCustomWritableComparator, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] m = value.toString().split("\t");
			if(m.length == 2){
				String line[] = m[1].split(",");
				context.write(new AgeCustomWritableComparator(Float.parseFloat(m[0]),Float.parseFloat(line[5])), new Text(m[1].toString()));
			}		
		}

	}



	public static class AgeReducer1 extends Reducer<LongWritable, Text, Text, Text> {

		HashMap<String, String> map=new HashMap<String, String>();
		private ArrayList<Text> listA = new ArrayList<Text>();
		private ArrayList<Text> listB = new ArrayList<Text>();


		public void setup(Context context) throws IOException {
			map = new HashMap<String,String>();
			Configuration config = context.getConfiguration();
			String mybusinessdataPath = config.get("data");

			Path pt = new Path("hdfs://cshadoop1"+mybusinessdataPath);
			FileSystem fs = FileSystem.get(config);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

			String line;
			while ((line = br.readLine()) != null){
				String[] arr=line.split(",");
				if(arr.length == 10){
					map.put(arr[0].trim(), arr[1]+":"+arr[3]+":"+arr[9]); 
				}
			}	 
		}


		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			listA.clear();
			listB.clear();

			for (Text t : values) {
				if (t.toString().charAt(0) == 'F') 
					listA.add(new Text(t.toString().substring(3)));
				else if (t.toString().charAt(0) == 'B') 
					listB.add(new Text(t.toString().substring(3)));
			}
			
			int count=0;
			float age=0;
			float maxAge;
			String res="";
			String[] details = null;

			if(!listA.isEmpty() && !listB.isEmpty()) {
				for(Text userData : listA) {
					String friendData[] = userData.toString().split(",");
					for(int i=0;i<friendData.length;i++) {
						if(map.containsKey(friendData[i])) {
							String[] a = map.get(friendData[i]).split(":");
							Date now = new Date();
							int nowMonth = now.getMonth()+1;
							int nowYear = now.getYear()+1900;
							String[] date = a[2].toString().split("/");
							int result = nowYear - Integer.parseInt(date[2]);

							if (Integer.parseInt(date[0]) > nowMonth) 
								result--;
							else if (Integer.parseInt(date[0]) == nowMonth) {
								int nowDay = now.getDate();

								if (Integer.parseInt(date[1]) > nowDay) 
									result--;
							}
							if(result > age){
								age = result;
							}
						}
					}
					
					maxAge=age;
					for(Text B:listB) {
						details=B.toString().split(",");
						res = B.toString()+","+new Text(new FloatWritable((float) maxAge).toString());
					}
				}
			}
			context.write(new Text(key.toString()), new Text(res));	
		}
	}
	
	
	
	public static class AgeReducer2 extends Reducer<AgeCustomWritableComparator, Text, Text, Text> {
		TreeMap<String,String> output=new TreeMap<String, String>();        
		public void reduce(AgeCustomWritableComparator key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for(Text t:values) {
				if(output.size()<10) {
					output.put(key.userId.toString(), t.toString());
					context.write(new Text(t.toString().split(",")[0]), new Text((t.toString().split(",")[2]+","+t.toString().split(",")[3]+","+t.toString().split(",")[4]+","+t.toString().split(",")[5]))); //****MODIFIED
					//context.write(new Text(), new Text(t)); //****MODIFIED
				}
			}
		}
	}


	public class AgePartitioner extends Partitioner<AgeCustomWritableComparator, Text>{
		@Override
		public int getPartition(AgeCustomWritableComparator age, Text nullWritable, int numPartitions) {
			return age.getAge().hashCode() % numPartitions;
		}
	}


	public static class AgeBasicKeySortComparator extends WritableComparator {

		public AgeBasicKeySortComparator() {
			super(AgeCustomWritableComparator.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return -1*((AgeCustomWritableComparator) w1).getAge().compareTo(((AgeCustomWritableComparator) w2).getAge());
		}
	}
	
	
	public static class AgeBasicGroupingComparator extends WritableComparator {
		public AgeBasicGroupingComparator() {
			super(AgeCustomWritableComparator.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return -1*((AgeCustomWritableComparator) w1).getAge().compareTo(((AgeCustomWritableComparator) w2).getAge());
		}
	}
	


	public static class AgeCustomWritableComparator implements  WritableComparable<AgeCustomWritableComparator>  {

		private Float userId;
		private Float age;

		public AgeCustomWritableComparator(Float user, Float age) {
			// TODO Auto-generated constructor stub
			this.userId=user;
			this.age=age;
		}
		public AgeCustomWritableComparator(){}
		
		public Float getUserId() {
			return userId;
		}
		public void setUserId(Float userId) {
			this.userId = userId;
		}
		
		public Float getAge() {
			return age;
		}
		public void setAge(Float age) {
			this.age = age;
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			userId = in.readFloat();
			age = in.readFloat();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeFloat(userId);;
			out.writeFloat(age);;
		}

		@Override
		public int compareTo(AgeCustomWritableComparator o) {
			int result = userId.compareTo(o.userId);
			if (result != 0) 
				return result;
			
			return this.age.compareTo(o.age);

		}
		
		@Override
		public String toString() {
			return userId.toString() + ":" + age.toString();
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final AgeCustomWritableComparator other = (AgeCustomWritableComparator) obj;
			if (this.userId != other.userId && (this.userId == null || !this.userId.equals(other.userId))) {
				return false;
			}
			if (this.age != other.age && (this.age == null || !this.age.equals(other.age))) {
				return false;
			}
			return true;
		}

	}	


	public static void main(String[] args) throws Exception  {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		conf.set("data",otherArgs[0]);
		if (otherArgs.length != 5) {
			System.err.println("Usage: JoinExample <inmemory input> <input > <input> <intermediate output> <output>");
			System.exit(-1);
		}

		Job job = new Job (conf, "join1");
		job.setJarByClass(MaxAgeGroup.class);
		job.setReducerClass(AgeReducer1.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		Path outputDirIntermediate1 = new Path(args[3] + "_temp1");
		Path outputDirIntermediate2 = new Path(args[4] + "_temp2");
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, FriendListMapper.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[2]),TextInputFormat.class, UserDetailsMapper.class );
		FileOutputFormat.setOutputPath(job,outputDirIntermediate1);
		int exitCode = job.waitForCompletion(true)?0:1;
		
		
		Job job1 = new Job(new Configuration(), "join2");
		job1.setJarByClass(MaxAgeGroup.class);
		job1.setMapperClass(AgeMapper.class);
		job1.setMapOutputKeyClass(AgeCustomWritableComparator.class);
		job1.setMapOutputValueClass(Text.class);
		
		job1.setReducerClass(AgeReducer2.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setPartitionerClass(AgePartitioner.class);
		job1.setSortComparatorClass(AgeBasicKeySortComparator.class);
		job1.setGroupingComparatorClass(AgeBasicGroupingComparator.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[3] + "_temp1"));		
		FileOutputFormat.setOutputPath(job1,outputDirIntermediate2);
		
		exitCode = job1.waitForCompletion(true) ? 0 : 1;


	}
}
