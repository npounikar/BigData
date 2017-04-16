package deepnet.project.crawler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import deepnet.project.bean.ConstantsBean;
import deepnet.project.bean.DocumentBean;
import deepnet.project.bean.PageContent;
import deepnet.project.classification.NaiveBayesClassifier;
import deepnet.project.files.FileUtility;
import deepnet.project.string.util.RandomNumberGenerator;


public class DarknetHTMLPageClassification {

	static ArrayList<String> paths = new ArrayList<String>();
	static HashMap<String, PageContent> homePageMap = new HashMap<String, PageContent>();

	public static void main(String[] args) {


		WebCrawler cu = new WebCrawler();
		PageContent homepage = cu.getPageContent(ConstantsBean.IN_URL);
		homePageMap.put(ConstantsBean.IN_URL, homepage);		
		paths.add(ConstantsBean.RESULT_DIRPATH);
		
		NaiveBayesClassifier nb = new NaiveBayesClassifier();
		DocumentBean docModel  = nb.performNaiveBayesClassification(homePageMap);

		// If the current doc is a cyber threat deepnet the links until the deepnetDepth
		if(homepage.getReferencedlinks().size() >= ConstantsBean.PER_LINK_ANCHOR_COUNT) {
			if(docModel.isThreat()) {

				// Objects
				FileUtility fu = new FileUtility();
				NaiveBayesClassifier model = docModel.getModel();
				PageContent pg = homePageMap.get(docModel.getDocId());
				List<String> homeLinks = pg.getReferencedlinks();
				
				// creating Random Numbers
				RandomNumberGenerator r = new RandomNumberGenerator();
				int[] randomNumbers = r.getRandomNumbers(homeLinks.size());


				// Setting the HashMap for the links that we have to classify
				HashMap<String, PageContent> linksMap = new HashMap<String, PageContent>();
				for(int j=0; j <randomNumbers.length; j++) {
					if(!homeLinks.get(randomNumbers[j]).equals("") || homeLinks.get(randomNumbers[j]) != null)
						linksMap.put(homeLinks.get(randomNumbers[j]), cu.getPageContent(homeLinks.get(randomNumbers[j])));
				}
				
				// start going in depth for classification for the found links on the homepage
				int mapCount = 0;
				Iterator<Entry<String, PageContent>> it = linksMap.entrySet().iterator();
				while (it.hasNext()) {
					System.out.println();
					@SuppressWarnings("rawtypes")
					Map.Entry pair = (Map.Entry)it.next();
					String docId = (String)pair.getKey();
					PageContent page = (PageContent)pair.getValue();

					String dirPath = paths.get(paths.size()-1)+"1D-"+mapCount+"L/";
					fu.createDir(dirPath);
					System.out.println("	"+dirPath +" : "+docId);
					fu.writeToFile(dirPath+"out.txt", page.getContent());
					String label = model.getLabelName(model.getDocumentLabel(dirPath+"out.txt"));
					System.out.println("	"+docId +" : "+label);
					mapCount++;	
				}
			}	
		}
	}
}


