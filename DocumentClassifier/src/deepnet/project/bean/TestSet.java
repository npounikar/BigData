package deepnet.project.bean;

import java.util.ArrayList;
import java.util.List;

public class TestSet {

	public List<String> getTestSetList() {
		
		//Training Set
		List<String> testFilePath = new ArrayList<String>();
		testFilePath.add(ConstantsBean.TESTDATA_DIRPATH+"CyberThreat");
		testFilePath.add(ConstantsBean.TESTDATA_DIRPATH+"DrugThreat");
		testFilePath.add(ConstantsBean.TESTDATA_DIRPATH+"WeaponThreat");
		testFilePath.add(ConstantsBean.TESTDATA_DIRPATH+"SecurityThreat");
		testFilePath.add(ConstantsBean.TESTDATA_DIRPATH+"TheftThreat");
		return testFilePath;
		
	}
}
