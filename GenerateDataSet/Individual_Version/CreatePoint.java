import java.io.FileWriter;
import java.util.Random;

public class CreatePoint {
	private static void CreateDataSet() {
		int count=0;
		float x=0;
		float y=0;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("point"); 
			 while(count<6000000){
				 count++;
				 x=new Random().nextFloat()*10000;
				 y=new Random().nextFloat()*10000;
				
				 lineRecordString=String.valueOf(x)+","+String.valueOf(y)+"\r\n";
				 fw.write(lineRecordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateDataSet();
	}


}
