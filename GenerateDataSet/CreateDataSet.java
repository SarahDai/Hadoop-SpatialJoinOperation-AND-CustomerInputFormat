import java.io.*;
import java.util.*;

public class CreateDatasets{
    public static void main(String[] args){
   		try{
   			int count = 6000000;
   	    	int max = 10000;
	    	FileWriter fw = new FileWriter("points.txt");

			for(int i = 1; i<count+1; i++){
				Random r = new Random();
			
				float x = r.nextFloat()*(max) + 1;
				float y = r.nextFloat()*(max) + 1;

				String record = String.valueOf(x) + "\t" + String.valueOf(y) +"\n";
				fw.write(record);			
			}
			fw.close();
		}catch(IOException e){
			e.printStackTrace();
		}

        //write the transaction file
		try{
			FileWriter fw1 = new FileWriter("rectangles.txt");
			int count1 = 3000000;
	    	int max1 = 10000;

			for(int i = 1; i<count1+1; i++){
				Random r = new Random();
				float x = r.nextFloat()*(max1) + 1;
				float y = r.nextFloat()*(max1) + 1;
				float x1 = r.nextFloat()*(5) + 1;
				float y1 = r.nextFloat()*(20) + 1;
				
				String record = "r" + String.valueOf(i) + "\t"+ String.valueOf(x) + "\t" + String.valueOf(y) + "\t" + String.valueOf(x1) + "\t" + String.valueOf(y1) +"\n";
				fw1.write(record);			
			}
			fw1.close();
		}catch(IOException e){
			e.printStackTrace();
		}
		
		try{
			int count2 = 1400000;
	    	FileWriter fw2 = new FileWriter("customer.txt");

			for(int i = 1; i<count2+1; i++){
				Random r = new Random();
				char[] s = new String("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ").toCharArray();
			    char[] gender = new String("fm").toCharArray();
				
				int name_length = r.nextInt(10)+1;
				int address_length = r.nextInt(11) + 10;
				char sex;
				int sex_index = r.nextInt(2);
				sex = gender[sex_index];
				float salary = r.nextFloat()*9900 + 100;

				//create user name randomly
            	String name = new String();
				for(int j = 0; j<name_length; j++){
					int letter = r.nextInt(51);
                	name=name+s[letter];
				}
				
				//create user address
				String address = new String();
				for(int j = 0; j<address_length; j++){
					int letter = r.nextInt(51);
					address=address+s[letter];
				}
				String record = new String();

					record = "{Customer ID: " + String.valueOf(i) + ",\n" 
				               +" Name: " + name + ",\n" 
				               +" Address: " + address + ",\n" 
				               +" Salary: " + String.valueOf(salary) + ",\n" 
				               +" Gender: " + String.valueOf(sex) +"\n" + "},\n";
				
				fw2.write(record);			
			}
			fw2.close();
		}catch(IOException e){
			e.printStackTrace();
		}
    }
}
