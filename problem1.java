import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class problem1 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		String ws;
		public void configure(JobConf job){
			ws = job.get("W");
		}
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)throws IOException{
			Text W = new Text();
			W.set(ws);
		    String window =W.toString();
			float x0 = Float.parseFloat(window.split(",")[0]);
			float y0 = Float.parseFloat(window.split(",")[1]);
			float x01 = Float.parseFloat(window.split(",")[2]);
			float y01 = Float.parseFloat(window.split(",")[3]);
			Text rec = new Text();
			String line = value.toString();
			String[] splits = line.split("\t");
			//this space is divided into 100*100 squares whose width is 100 and height is 100.
			if(splits.length!=2){
				float x1 = Float.parseFloat(splits[1]);
				float y1 = Float.parseFloat(splits[2]) - Float.parseFloat(splits[4]);
				float x2 = Float.parseFloat(splits[1]) + Float.parseFloat(splits[3]);
				float y2 = Float.parseFloat(splits[2]);
				if(!(y1>(y01)||y2<y0||x1>(x01)||x2<x0)){
					rec.set(String.valueOf((int)x1/100)+ "," + String.valueOf((int)y1/100));
					output.collect(rec,value);
					if(((int)x1/100)!=((int)x2/100)){
						if(((int)y1/100)!=((int)y2/100)){
							rec.set(String.valueOf((int)x2/100)+ "," + String.valueOf((int)y2/100));
							output.collect(rec,value);
						}
						rec.set(String.valueOf((int)x2/100)+ "," + String.valueOf((int)y1/100));
						output.collect(rec,value);
					}
					if(((int)y1/100)!=((int)y2/100)){
						rec.set(String.valueOf((int)x1/100)+ "," + String.valueOf((int)y2/100));
						output.collect(rec,value);
					}				
				}
			}else{
				float px = Float.parseFloat(splits[0]);
				float py = Float.parseFloat(splits[1]);
				if(px>=x0 & px<=(x01) & py>=y0 & py<=(y01)){
					rec.set(String.valueOf((int)px/100) + "," + String.valueOf((int)py/100));
					output.collect(rec,value);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)throws IOException{
			List<String> points = new ArrayList<String>();
			List<String> rects = new ArrayList<String>();
			while (values.hasNext()){
				String value = values.next().toString();
			    if(value.split("\t").length==2){
			    	points.add(value);
			    }else{
			    	rects.add(value);
			    }
			}
			for(String tmp1:rects){
				//split the rectangle data
				String[] rectangle = tmp1.split("\t");
				String rname = rectangle[0];
				
				float rx0 = Float.parseFloat(rectangle[1]);
				float ry0 = Float.parseFloat(rectangle[2]) - Float.parseFloat(rectangle[4]);;
				float rx1 = Float.parseFloat(rectangle[1]) + Float.parseFloat(rectangle[3]);
				float ry1 = Float.parseFloat(rectangle[2]);
				//for each point, check if it is in the rectangle
				for(String tmp2:points){
					float px = Float.parseFloat(tmp2.split("\t")[0]);
					float py = Float.parseFloat(tmp2.split("\t")[1]);
					if(px >= rx0 & px<=rx1 & py>=ry0 & py<=ry1){
						Text text = new Text();
						text.set("<" + rname + "," + "(" + String.valueOf(px) + "," + String.valueOf(py)+ ")>");
						key.set(rname);
						output.collect(text,new Text());
					}
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
      JobConf conf = new JobConf(problem1.class);
      //conf = (JobConf) getConf();
      conf.set("W", "1,3,10,30");
      conf.setJobName("problem1");
      
      

      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(Text.class);

      conf.setMapperClass(Map.class);
      //conf.setCombinerClass(Reduce.class);
      conf.setReducerClass(Reduce.class);

      conf.setInputFormat(TextInputFormat.class);
      conf.setOutputFormat(TextOutputFormat.class);
	
      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	  FileOutputFormat.setOutputPath(conf, new Path(args[1]));

      JobClient.runJob(conf);
    }	
}
