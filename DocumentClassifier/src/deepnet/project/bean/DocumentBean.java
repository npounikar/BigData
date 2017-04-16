package deepnet.project.bean;

import deepnet.project.classification.NaiveBayesClassifier;

public class DocumentBean {
	String docId;
	String label;
	boolean isThreat;
	NaiveBayesClassifier model;
	
	
	public DocumentBean() {
		this.label = "UnDefinedCategory";
		this.isThreat = false;
	}
	
	DocumentBean(String docId, String label, NaiveBayesClassifier model) {
		this.docId = docId;
		this.label = label;
		this.model = model;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}
	
	public boolean isThreat() {
		return isThreat;
	}

	public void setThreat(boolean isCyberthreat) {
		this.isThreat = isCyberthreat;
	}

	public NaiveBayesClassifier getModel() {
		return model;
	}

	public void setModel(NaiveBayesClassifier model) {
		this.model = model;
	}
	
	

}
