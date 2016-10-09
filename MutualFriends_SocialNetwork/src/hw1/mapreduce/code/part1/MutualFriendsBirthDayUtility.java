package hw1.mapreduce.code.part1;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MutualFriendsBirthDayUtility extends Configured implements Tool {

	static HashMap<String, String> map = new HashMap<String, String>();
	
	public static class MutualFriendsUtilityMapper extends Mapper<LongWritable, Text, Text, Text> {
		

		Long tempVar = new Long(-1L);
		Long userA = new Long(-1L);
		Long userB = new Long(-1L);
		Long currentUser = new Long(-1L);
		
		public void setup(Context context) throws IOException {
			Configuration config = context.getConfiguration();
			userA = Long.parseLong(config.get("userA"));
			userB = Long.parseLong(config.get("userB"));
			
			/* In- Memory */
			Path[] userData = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			FileSystem fs = FileSystem.get(config);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(userData[0])));
			String line = br.readLine();
			while (line != null) {
				System.out.println("=="+line);
				String[] myArr = line.split(",");
				if (myArr.length == 10) {
					String data = myArr[1] + ":" + myArr[9];
					map.put(myArr[0].trim(), data);
				}
				line = br.readLine();
			}
		}

		
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] lineContent = value.toString().split("\t");
			currentUser = Long.parseLong(lineContent[0]);
			
			if(lineContent.length == 2) {
				
				String friends = lineContent[1];
				if((currentUser.equals(userA)) || (currentUser.equals(userB))) {
					
					if(currentUser.equals(userA))
						tempVar = userB;
					else
						tempVar = userA;
					
					UserAsKeyWritable fData = null;
					String dataStr = "";
					if(currentUser.compareTo(tempVar) < 0 ) {
						fData = new UserAsKeyWritable(currentUser, tempVar);
						dataStr = fData.toString();
						context.write(new Text(dataStr), new Text(friends) );
						
					} else {
						fData = new UserAsKeyWritable(tempVar, currentUser);
						dataStr = fData.toString();
						context.write(new Text(dataStr), new Text(friends) );
					}   
				}
			}
		}
	}
	
	
	
	public static class MutualFriendsUtilityReducer extends Reducer<Text, Text, Text, Text> {

		private HashSet<Integer> getMutualFriends(String str1, String str2) {

			HashSet<Integer> set1 = new HashSet<Integer>();
			HashSet<Integer> set2 = new HashSet<Integer>();
			if (str1 != null) {
				String[] arr = str1.split(",");
				for (int i = 0; i < arr.length; i++) {
					set1.add(Integer.parseInt(arr[i]));
				}
			}

			if (str2 != null) {
				String[] arr2 = str2.split(",");
				for (int i = 0; i < arr2.length; i++) {
					if (set1.contains(Integer.parseInt(arr2[i]))) {
						set2.add(Integer.parseInt(arr2[i]));
					}
				}
			}
			return set2;
		}
		
		
		private ArrayList<String> getFriendData(HashSet<Integer> mutualSet) {
			
			Text mutual = new Text(StringUtils.join(", ", mutualSet.toArray()));
			String[] splitArr = mutual.toString().split(", ");
			ArrayList<String> friendData = new ArrayList<String>();
			if (map != null && !map.isEmpty()) {
				for (String s : splitArr) {
					if (map.containsKey(s)) {
						friendData.add(map.get(s));
						map.remove(s);
					}
				}
			}
			return friendData;
		}
		
		
		private String getResultString(ArrayList<String> friendData) {
			String resStr = "[";
			for (String curr : friendData) 
				resStr = resStr + curr + ",";
			
			resStr = resStr.substring(0, resStr.length() - 1);
			resStr = resStr + "]";
			return resStr;
		}
		
		
		
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String[] combined = new String[2];
			int cur = 0;
			for (Text value : values) 
				combined[cur++] = value.toString();

			if (combined[0] != null) 
				combined[0] = combined[0].replaceAll("[^0-9,]", "");

			if (combined[1] != null) {
				combined[1] = combined[1].replaceAll("[^0-9,]", "");
			}

			HashSet<Integer> mutualSet = getMutualFriends(combined[0], combined[1]);
			ArrayList<String> friendData = getFriendData(mutualSet);
			String resultString = getResultString(friendData);

			context.write(key, new Text(resultString));
		}
		
	}
	
	
	
	public static class UserAsKeyWritable implements WritableComparable<UserAsKeyWritable> {

		private Long userId, friendId;

		public UserAsKeyWritable(Long user, Long friend1) {
			this.userId = user;
			this.friendId = friend1;
		}

		public UserAsKeyWritable() {
		}

		public void readFields(DataInput in) throws IOException {
			userId = in.readLong();
			friendId = in.readLong();
		}

		public void write(DataOutput out) throws IOException {
			out.writeLong(userId);
			out.writeLong(friendId);
		}

		public int compareTo(UserAsKeyWritable o) {
			int result = userId.compareTo(o.userId);
			if (result != 0) {
				return result;
			}
			return this.friendId.compareTo(o.friendId);
		}

		public String toString() {
			return userId.toString() + "  " + friendId.toString();
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
	
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new MutualFriendsBirthDayUtility(), args);
		System.exit(code);
	}

	
	public int run(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		if (args.length != 5) {
			System.err.println("Error : Input");
			System.exit(2);
		}

		conf.set("userA", args[0]);
		conf.set("userB", args[1]);

		Job job = new Job(conf, "HW1_3");
		job.setJarByClass(MutualFriendsBirthDayUtility.class);

		job.setMapperClass(MutualFriendsUtilityMapper.class);
		job.setReducerClass(MutualFriendsUtilityReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		DistributedCache.addCacheFile(new Path(args[3]).toUri(), job.getConfiguration());
		
		boolean sucess = job.waitForCompletion(true);
		return (sucess ? 0 : 1);
		
	}

}
