package deepnet.project.classification;
import java.io.File;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import deepnet.project.bean.ConstantsBean;
import deepnet.project.bean.DocumentBean;
import deepnet.project.bean.PageContent;
import deepnet.project.bean.TestSet;
import deepnet.project.bean.TrainingSet;
import deepnet.project.files.FileUtility;

public class NaiveBayesClassifier {

	static boolean isAvoidWordsInitiated;
	static boolean isTrainDataCreated;	
	static int wordCountArray[];
	static int vocabSize;
	static double classPriorProbability[]; 
	static HashSet<String> avoidWordsSet = new HashSet<String>();
	static HashMap<String,List<Integer>> wordCountMap = new HashMap<String,List<Integer>>();
	int totalClasses;
	List<String> addressList;


	int getLabel(String path){
		HashMap<String,Integer> docCountMap = new HashMap<String,Integer>();
		InputReaderImplementation d = new InputReaderImplementation();
		String words[] = d.getData(path);
		if(words != null) {
			for(int j=0; j<words.length; j++){
				String curr = clean(words[j]);
				if(curr.length()>0 && !avoidWordsSet.contains(curr)){
					if(!docCountMap.containsKey(curr))
						docCountMap.put(curr,1);
					else
						docCountMap.put(curr,docCountMap.get(curr)+1);
				}
			}
		}
		return computeProbabilities(totalClasses,docCountMap);
	}


	public int getDocumentLabel(String docPath){
		if(!isAvoidWordsInitiated) {
			AvoidWordReaderImplementation awr = new AvoidWordReaderImplementation();
			String toAvoid[] = awr.getData(ConstantsBean.STOPWORDS_PATH);
			for(int i=0; i<toAvoid.length; i++)
				avoidWordsSet.add(toAvoid[i]);
			isAvoidWordsInitiated = true;
		}

		if(!isTrainDataCreated){	
			wordCountMap = createWordCountMap(addressList);
			isTrainDataCreated = true;
			vocabSize = wordCountMap.size();
		}
		return getLabel(docPath);
	}


	HashMap<String,List<Integer>> createWordCountMap(List<String> pathList){
		HashMap<String,List<Integer>> wordCountMap = new HashMap<String,List<Integer>>();
		try {
			if(pathList==null)
				throw new Exception("pathList is NULL.");

			if(pathList.size()>totalClasses)
				throw new Exception("pathList.size exceeds totalClasses.");

			InputReader fileReader = new InputReaderImplementation();
			int docCountKeeper[] = new int[totalClasses];
			for(int i=0; i<pathList.size(); i++){
				int totalFiles = new File(pathList.get(i)).list().length;
				docCountKeeper[i] = totalFiles;
				File folder = new File(pathList.get(i));
				File[] listOfFiles = folder.listFiles();
				for (File file : listOfFiles) {
					if (file.isFile()) {
						String currFilePath = pathList.get(i)+"/"+file.getName();
						String words[] = fileReader.getData(currFilePath);
						processWords(words,i,wordCountMap,totalClasses);
					}
				}
			}
			computePriorProbability(docCountKeeper);
			//printMap(wordCountMap);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return wordCountMap;
	}


	int computeProbabilities(int totalClasses, HashMap<String,Integer> docCountMap){
		BigDecimal max = new BigDecimal(-1.0);
		int maxIdx = -1;
		for(int i=0; i<totalClasses; i++){
			int totalWordsInClass = wordCountArray[i];
			int denominator = totalWordsInClass+vocabSize;
			Set<String> s = docCountMap.keySet();
			Iterator<String> itr = s.iterator();
			BigDecimal bg1 = new BigDecimal(1.0);
			while(itr.hasNext()){
				String key = itr.next();
				List<Integer> tempL = wordCountMap.get(key);
				int currDocWordCount = 0;
				if(tempL==null)
					currDocWordCount=1;
				else
					currDocWordCount = tempL.get(i)+1;
				double probWord = (currDocWordCount)/(1.0 * denominator);
				BigDecimal bg2 = new BigDecimal(probWord);
				bg1 = bg1.multiply(bg2);
			}

			bg1 = bg1.multiply(new BigDecimal(classPriorProbability[i]));
			if(bg1.compareTo(max) > 0){
				max = bg1;
				maxIdx	= i;	
			}
		}
		return maxIdx;
	}



	void processWords(String words[], int classVal, HashMap<String,List<Integer>> wordCountMap, int totalClasses){
		if(words != null) {
			for(int i=0; i<words.length; i++){
				words[i] = clean(words[i]);
				if(words[i].length() > 0 && !avoidWordsSet.contains(words[i])){
					wordCountArray[classVal]++;
					if(!wordCountMap.containsKey(words[i])){
						List<Integer> countListForClasses = new ArrayList<Integer>();
						for(int j=0; j<totalClasses; j++)
							countListForClasses.add(0);
						countListForClasses.set(classVal,countListForClasses.get(classVal)+1);
						wordCountMap.put(words[i],countListForClasses);
					}
					else{
						List<Integer> countListForClasses = wordCountMap.get(words[i]);
						countListForClasses.set(classVal,countListForClasses.get(classVal)+1);
					}
				}
			}
		}
	}



	public  DocumentBean  performNaiveBayesClassification(HashMap<String, PageContent> homePageMap) {

		// Objects
		TrainingSet train = new TrainingSet();
		TestSet test = new TestSet();
		FileUtility fu = new FileUtility();
		DocumentBean doc = null;

		//Train and Test Set
		List<String> trainingFilePath = train.getTrainingSetList();
		List<String> testFilePath = test.getTestSetList();

		//Create Model on training data && Computing Accuracy on Test data
		NaiveBayesClassifier nbObj = new NaiveBayesClassifier(trainingFilePath, ConstantsBean.NO_OF_TARGET_CLASSES);
		nbObj.computeAccuracy(testFilePath);

		// traverse the map and write the content into file and then get the classified label to the document
		if(homePageMap.size() == 1) {

			String docId = "";
			String label = "";
			doc = new DocumentBean();

			Iterator<Entry<String, PageContent>> it = homePageMap.entrySet().iterator();
			while (it.hasNext()) {

				@SuppressWarnings("rawtypes")
				Map.Entry pair = (Map.Entry)it.next();
				docId = (String)pair.getKey();
				PageContent page = (PageContent)pair.getValue();

				fu.writeToFile(ConstantsBean.RESULT_DIRPATH+"out.txt", page.getContent());
				label = nbObj.getLabelName(nbObj.getDocumentLabel(ConstantsBean.RESULT_DIRPATH+"out.txt"));

			}

			//setting the document bean object for homepage
			doc.setDocId(docId);
			doc.setLabel(label);
			doc.setModel(nbObj);
			if(!label.equals("UnDefinedCategory")) 
				doc.setThreat(true);

			System.out.println(doc.getDocId() +" : "+doc.getLabel());
		}

		return doc;
	}


	public String getLabelName(int classNum) {
		switch(classNum) {
		case 0: return "CyberThreat";
		case 1: return "DrugThreat";
		case 2: return "WeaponThreat";
		case 3: return "SecurityThreat";
		case 4: return "TheftThreat";
		}
		return "UnDefinedCategory";
	}


	void computePriorProbability(int docNumberKeeper[]){
		int totalDocuments = 0;
		for(int i=0; i<totalClasses; i++){
			totalDocuments += docNumberKeeper[i];
			classPriorProbability[i] = 1;
		}
		if(totalDocuments > 0){
			for(int i=0; i<totalClasses; i++){
				classPriorProbability[i] =  docNumberKeeper[i]/(1.0*totalDocuments);
			}
		}
	}

	void computeAccuracy(List<String> pathList){
		int correctCount = 0;
		int totalCount = 0;
		for(int i=0; i<pathList.size(); i++){
			File folder = new File(pathList.get(i));
			File[] listOfFiles = folder.listFiles();
			totalCount += listOfFiles.length;
			for (File file : listOfFiles) {
				if (file.isFile()) {
					String currFilePath = pathList.get(i)+"/"+file.getName();
					int classPredict = getDocumentLabel(currFilePath);
					if(classPredict == i)
						correctCount++;
				}
			}
		}
		double accuracy = (correctCount/(1.0*totalCount)) * 100;
		DecimalFormat df = new DecimalFormat("#.00");
		System.out.println("Accuracy : "+ Double.valueOf(df.format(accuracy))+" %");
	}


	String clean(String word){
		word = word.toLowerCase();
		StringBuilder newWord = new StringBuilder();
		for(int i=0; i<word.length(); i++){
			if(word.charAt(i) >= 'a' && word.charAt(i) <= 'z')
				newWord.append(word.charAt(i));
		}
		return newWord.toString();
	}


	NaiveBayesClassifier(List<String> pathList, int totalClasses) {
		this.addressList = pathList;
		this.totalClasses = totalClasses;
		wordCountArray = new int[totalClasses];
		classPriorProbability = new double[totalClasses];
	}

	public NaiveBayesClassifier() {
	}


}
