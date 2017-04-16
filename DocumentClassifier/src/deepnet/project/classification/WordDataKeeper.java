package deepnet.project.classification;
import java.util.*;

public class WordDataKeeper {
	
	String word;
	HashMap<Integer,Integer> classCountMap = new HashMap<Integer,Integer>();
	
	void incrementClassCount(int val) {
		if(!classCountMap.containsKey(val))
			classCountMap.put(val,1);
		else
			classCountMap.put(val,classCountMap.get(val)+1);
	}
	
	WordDataKeeper(String word){
		this.word = word;
	}
	
}
