package deepnet.project.string.util;

import java.util.ArrayList;
import java.util.HashSet;

import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.englishStemmer;

import deepnet.project.bean.ConstantsBean;
import deepnet.project.classification.AvoidWordReaderImplementation;
import deepnet.project.classification.InputReader;

public class StringStemmer {
	
	HashSet<String> stopWords = new HashSet<String>();
	SnowballProgram stemmer;
	
	public StringStemmer() {
		stemmer = new englishStemmer();
		InputReader awr = new AvoidWordReaderImplementation();
		String toAvoid[] = awr.getData(ConstantsBean.STOPWORDS_PATH);
		for(int i=0; i<toAvoid.length; i++)
			stopWords.add(toAvoid[i]);
	}
	
	
	public String stem(String word) {
		stemmer.setCurrent(word);
		((englishStemmer) stemmer).stem();
		return stemmer.getCurrent();
	}
	
	
	public ArrayList<String> removeStopWords(String[] content) {
		ArrayList<String> newContent = new ArrayList<String>();
		for(int i = 0; i < content.length; i++) {
			if(!stopWords.contains(content[i])) 
				newContent.add(content[i]);
		}
		return newContent;
	}
	
	

}
