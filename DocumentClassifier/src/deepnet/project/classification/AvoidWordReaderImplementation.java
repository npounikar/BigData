package deepnet.project.classification;
import java.io.File;
import java.util.Scanner;


public class AvoidWordReaderImplementation implements InputReader{
	
	@Override
	public String[] getData(Object loc) {
		
		String result[] = null;
		String address = (String)loc;
		
		Scanner scanner = null;
		try {
			scanner = new Scanner(new File(address)).useDelimiter("\\Z");
			while(scanner.hasNext()) {
				result = scanner.next().split("\\s+");
			}
		} catch(Exception e){
			e.printStackTrace();
		} finally{
			scanner.close();
		}
		
		return result;
	}
}
