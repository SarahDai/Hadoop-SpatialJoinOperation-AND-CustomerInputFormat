import java.io.FileWriter;
import java.util.Random;

public class CreateCustomers {
	private static void CreateDataSet() {
		int ID=0;
		String name=null;
		String addr=null;
		float salary=0;
		String gender=null;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("customers"); 
			 while(ID<1500000){
				 ID++;
				 name=getRandomString(10);
				 addr=getRandomString(15);
				 salary=new Random().nextFloat()*9900+100;
				 gender=(new Random().nextInt(1000) % 2 == 1)?"male":"female";
				 lineRecordString="{Customer ID:"+String.valueOf(ID)+",\r\n Name:"+name+",\r\n Address:"+addr+",\r\n Salary:"+String.valueOf(salary)+",\r\n Gender:"+gender+"\r\n},\r\n";
				 fw.write(lineRecordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }
		
	public static String getRandomString(int length) { 
	    String base = "abcdefghijklmnopqrstuvwxyz";   
	    Random random = new Random();   
	    StringBuffer sb = new StringBuffer();   
	    for (int i = 0; i < length; i++) {   
	        int number = random.nextInt(base.length());   
	        sb.append(base.charAt(number));   
	    }   
	    return sb.toString();   
	 }  

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CreateDataSet();
	}


}
