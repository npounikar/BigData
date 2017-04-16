package deepnet.project.crawler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import deepnet.project.bean.PageContent;


public class WebCrawler {

	
	public PageContent getPageContent(String url) {
		return crawl(url);
	}
	
	
	private PageContent crawl(String inUrl) {
		Document doc = loadDocumentFromWeb(inUrl);
		String content = getContent(doc);
		List<String> links = getLinks(doc);
		return new PageContent(inUrl, content, links);
	}
	
	
	private Document loadDocumentFromWeb(String inUrl) {
		Document document = null;
		try {
			document = Jsoup.connect(inUrl).get();
		} catch (IOException e) {
			System.out.println("InValid URL..");
		}
		return document;
	}

	
	private List<String> getLinks(Document doc) {
		List<String> anchorHrefs = new ArrayList<String>();
		Elements links = doc.select("a");
		for(Element e : links) {
			if(!e.attr("abs:href").equals(""))
				anchorHrefs.add(e.attr("abs:href"));	
		}
		return anchorHrefs;
	}


	private String getContent(Document doc) {
		return doc.select("p").text();	
	}

	
}
