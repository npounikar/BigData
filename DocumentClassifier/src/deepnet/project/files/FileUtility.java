package deepnet.project.files;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import deepnet.project.string.util.StringStemmer;

public class FileUtility {

	public void writeToFile(String path, String content) {
		
		PrintWriter writer = null;
		String words[] = content.split(" ");
		String stemmedContent = refineContent(words);
		try{
		    writer = new PrintWriter(path, "UTF-8");
		    writer.println(stemmedContent);
		    writer.close();
		} catch (IOException e) {
			System.out.println("File IO error");
		} finally {
			writer.close();
		}
	}
	
	
	private String refineContent(String[] words) {
		
		StringBuffer sb = new StringBuffer();
		StringStemmer stemmer = new StringStemmer();
		
		ArrayList<String> newContent = stemmer.removeStopWords(words);
		for(int i = 0; i < newContent.size(); i++) {
			sb.append(stemmer.stem(newContent.get(i)));
			sb.append(" ");
		}
		return sb.toString();
	}


	public void createDir(String path) {
		File file = new File(path);
		if(!file.exists())
			file.mkdir();
	}

}
