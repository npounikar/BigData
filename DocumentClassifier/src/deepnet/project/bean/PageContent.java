package deepnet.project.bean;

import java.util.List;

public class PageContent {
	
	String linkId;
	String content;
	List<String> referencedlinks;
	
	public PageContent() {
		
	}
	
	public PageContent(String selfLink, String content, List<String> referencedlinks) {
		this.linkId = selfLink;
		this.content = content;
		this.referencedlinks = referencedlinks;
		
	}

	public String getLinkId() {
		return linkId;
	}

	public void setLinkId(String linkId) {
		this.linkId = linkId;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public List<String> getReferencedlinks() {
		return referencedlinks;
	}

	public void setReferencedlinks(List<String> referencedlinks) {
		this.referencedlinks = referencedlinks;
	}
	
	

}
