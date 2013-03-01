import java.io.FileWriter;
import java.util.Random;

public class CreateRectangle {
	private static void CreateDataSet() {
		int index=0;
		float x=0;
		float y=0;		
		float height=0;
		float width=0;
		String lineRecordString;
		 try { 
			 FileWriter fw = new FileWriter("rectangle"); 
			 while(index<6000000){
				 index++;
				 x=new Random().nextFloat()*9995;
				 y=new Random().nextFloat()*9980+20;
				 height=new Random().nextFloat()*19+1;
				 width=new Random().nextFloat()*4+1;
				 lineRecordString="r"+String.valueOf(index)+","+String.valueOf(x)+","+String.valueOf(y)+","+String.valueOf(width)+","+String.valueOf(height)+"\r\n";
				 fw.write(lineRecordString);  			 
			 }
			 fw.close(); 
			 } 
		 catch (Exception e) { 
			 } 
		 }


	public static void main(String[] args) 
	{
		CreateDataSet();
	}


}
