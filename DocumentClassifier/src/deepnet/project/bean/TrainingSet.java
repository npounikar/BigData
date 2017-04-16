package deepnet.project.bean;

import java.util.ArrayList;
import java.util.List;

public class TrainingSet {
	
	public List<String> getTrainingSetList() {
		
			//Training Set
			List<String> trainingFilePath = new ArrayList<String>();
			trainingFilePath.add(ConstantsBean.TRAININGDATA_DIRPATH+"CyberThreat");
			trainingFilePath.add(ConstantsBean.TRAININGDATA_DIRPATH+"DrugThreat");
			trainingFilePath.add(ConstantsBean.TRAININGDATA_DIRPATH+"WeaponThreat");
			trainingFilePath.add(ConstantsBean.TRAININGDATA_DIRPATH+"SecurityThreat");
			trainingFilePath.add(ConstantsBean.TRAININGDATA_DIRPATH+"TheftThreat");
			return trainingFilePath;
			
	}
}
