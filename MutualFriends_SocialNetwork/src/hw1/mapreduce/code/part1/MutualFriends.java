package hw1.mapreduce.code.part1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriends extends Configured implements Tool{


	/* Mapper Class*/
	public static class MutualFriendsMapper extends Mapper<LongWritable, Text, UserAsKeyWritable, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Long currentUser = new Long(-1L);
			String[] lineContent = value.toString().split("\t");

			if(lineContent.length == 2) {

				currentUser = Long.parseLong(lineContent[0]);
				String friends[] = lineContent[1].toString().split(","); 

				if(friends.length>0) {
					for(int i=0; i < friends.length; i++) {
						Long currentFriend = Long.parseLong(friends[i]);
						String friendStr = "";

						for(int j = 0; j < friends.length; j++) {
							if(j != i) 
								friendStr += friends[j]+", ";
						}

						if(friendStr.length() > 2) {
							if(currentUser < currentFriend)
								context.write(new UserAsKeyWritable(currentUser, currentFriend), new Text(friendStr.substring(0, friendStr.length() - 2)));
							else
								context.write(new UserAsKeyWritable(currentFriend, currentUser), new Text(friendStr.substring(0, friendStr.length() - 2)));
						}

					}
				}

			}
		}
	}


	public static class MutualFriendsReducer extends Reducer<UserAsKeyWritable, Text, UserAsKeyWritable, Text> {

		public void reduce(UserAsKeyWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			StringBuffer result = new StringBuffer();
			Set<String> set1 = new HashSet<String>();
			String[][] friendlist = new String[2][];
			int i = 0;
			for(Text value : values) {
				friendlist[i] = value.toString().split(", ");	
				i++;				
			}
			for(int j = 0 ; j <friendlist[0].length; j++) {
				set1.add(friendlist[0][j]);
			}

			if(friendlist[1] != null ) {
				for(int j = 0 ; j <friendlist[1].length; j++) {
					if(set1.contains(friendlist[1][j]))
						result.append(friendlist[1][j]+", ");
				}
			}
	
			if(result.length() > 2) {
				context.write(key, new Text(result.substring(0, result.length()-2)));
			}
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
		int toolRunner = ToolRunner.run(new Configuration(), new MutualFriends(), args);
		System.exit(toolRunner);

	}

	@Override
	public int run(String[] otherArgs) throws Exception {
		Configuration conf = new Configuration();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Error Message");
			System.exit(2);
		}

		Job job = new Job(conf, "HW1_1");
		job.setJarByClass(MutualFriends.class);

		job.setMapperClass(MutualFriendsMapper.class);
		job.setReducerClass(MutualFriendsReducer.class);
		job.setOutputKeyClass(UserAsKeyWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);

	}

}
