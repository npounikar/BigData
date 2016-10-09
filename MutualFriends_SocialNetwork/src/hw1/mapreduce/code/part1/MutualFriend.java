package hw1.mapreduce.code.part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriend extends Configured implements Tool{

	/* Mapper Class*/
	public static class MutualFriendMapper extends Mapper<LongWritable, Text, UserAsKeyWritable, Text> {
		

		Long tempVar = new Long(-1L);
		Long userA = new Long(-1L);
		Long userB = new Long(-1L);
		Long currentUser = new Long(-1L);
		
		public void setup(Context context) {
			Configuration config = context.getConfiguration();
			userA = Long.parseLong(config.get("userA"));
			userB = Long.parseLong(config.get("userB"));
		}

		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lineContent = value.toString().split("\t");
			currentUser = Long.parseLong(lineContent[0]);
			
			if(lineContent.length == 2) {
				
				String friends = lineContent[1];
				if((currentUser.equals(userA)) || (currentUser.equals(userB))) {
					
					if(currentUser.equals(userA))
						tempVar = userB;
					else
						tempVar = userA;
					if(currentUser.compareTo(tempVar) < 0 ) 
						context.write(new UserAsKeyWritable(currentUser,tempVar), new Text(friends) );
					else
						context.write(new UserAsKeyWritable(tempVar,currentUser), new Text(friends) );
					           
				}

			}
		}
	}


	public static class MutualFriendReducer extends Reducer<UserAsKeyWritable, Text, UserAsKeyWritable, Text> {
		
		HashMap<String, Integer> hash = new HashMap<String, Integer>();
		
		private HashSet<Integer> getMutualFriends(String str1, String str2) {
			HashSet<Integer> set1 = new HashSet<Integer>();
			HashSet<Integer> set2 = new HashSet<Integer>();

			String[] arr = str1.split(",");
			for(int i=0; i<arr.length; i++)
				set1.add(Integer.parseInt(arr[i]));

			if(str2 != null) {
				String[] arr2 = str2.split(",");
				for(int i=0;i<arr2.length;i++) {
					if(set1.contains(Integer.parseInt(arr2[i])))
						set2.add(Integer.parseInt(arr2[i]));
				}
			}

			return set2;
		}

		public void reduce(UserAsKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			String[] combinedData = new String[2];
			int current = 0;
			for(Text value : values) 
				combinedData[current++] = value.toString();
			
			combinedData[0] = combinedData[0].replaceAll("[^0-9,]", "");
			if(combinedData[1]!=null)
				combinedData[1] = combinedData[1].replaceAll("[^0-9,]", "");

			HashSet<Integer> mSet = getMutualFriends(combinedData[0], combinedData[1]);
			context.write(key, new Text(StringUtils.join(",", mSet.toArray())));

		}
	}
	
	
	
	public static class UserAsKeyWritable implements  WritableComparable<UserAsKeyWritable>  {

		private Long userId, friendId;

		public UserAsKeyWritable(Long user, Long friend1) {
			this.userId = user;
			this.friendId = friend1;
		}
		
		public UserAsKeyWritable(){}
		
		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			out.writeLong(friendId);
		}

		
		public String toString() {
			return userId.toString() + "," + friendId.toString();
		}
		
		public int compareTo(UserAsKeyWritable o) {
			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
		}
		
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			}
			if (getClass() != obj.getClass()) {
				return false;
			}
			final UserAsKeyWritable other = (UserAsKeyWritable) obj;
			if (this.userId != other.userId && (this.userId == null || !this.userId.equals(other.userId))) {
				return false;
			}
			if (this.friendId != other.friendId && (this.friendId == null || !this.friendId.equals(other.friendId))) {
				return false;
			}
			return true;
		}

	}

	
	
	//Main method - Entry point
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new MutualFriend(), args);
		System.exit(code);

	}

	@Override
	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 4) {
			System.err.println("Usage: Error Message");
			System.exit(2);
		}

		conf.set("userA", otherArgs[0]);
		conf.set("userB", otherArgs[1]);

		Job job = new Job(conf, "HW1_2");
		job.setJarByClass(MutualFriend.class);


		job.setMapperClass(MutualFriendMapper.class);
		job.setReducerClass(MutualFriendReducer.class);
		job.setOutputKeyClass(UserAsKeyWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);

	}

}
