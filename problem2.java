import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
	
public class problem2 {

	public static class MyInputFormat extends FileInputFormat<LongWritable,Text>
	{    	
		/*@Override
		protected boolean isSplitable(FileSystem fs, Path file){
			return false;
		}*/
		@Override
		public RecordReader<LongWritable,Text> getRecordReader(InputSplit input,JobConf job,Reporter reporter) throws IOException
		{
			reporter.setStatus(input.toString());
			return new CustomerRecordReader(job, (FileSplit)input);  
		}
	}

	public static class CustomerRecordReader implements RecordReader<LongWritable, Text>
	{
		private CompressionCodecFactory compressionCodecs = null;
		private long start;
		private long pos;
		private long end;
		private LineReader in;
		int maxLineLength;
		private Seekable filePosition;
		private CompressionCodec codec;
        private Decompressor decompressor;
	
		  /**
		   * A class that provides a line reader from an input stream.
		   * @deprecated Use {@link org.apache.hadoop.util.LineReader} instead.
		   */
		@Deprecated
		public static class LineReader extends org.apache.hadoop.util.LineReader 
		{
			LineReader(InputStream in) 
			{
		  		super(in);
			}
			LineReader(InputStream in, int bufferSize) 
			{
			    super(in, bufferSize);
			}
			public LineReader(InputStream in, Configuration conf) throws IOException 
			{
			    super(in, conf);
			}
		}

		public CustomerRecordReader(Configuration job, FileSplit split) throws IOException 
		{
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
			start = split.getStart();
			end = start + split.getLength();
			final Path file = split.getPath();
			compressionCodecs = new CompressionCodecFactory(job);
			codec = compressionCodecs.getCodec(file);

			// open the file and seek to the start of the split
			FileSystem fs = file.getFileSystem(job);
			FSDataInputStream fileIn = fs.open(split.getPath());

			if (isCompressedInput()) {
			  decompressor = CodecPool.getDecompressor(codec);
			  if (codec instanceof SplittableCompressionCodec) {
				final SplitCompressionInputStream cIn =
				  ((SplittableCompressionCodec)codec).createInputStream(
				    fileIn, decompressor, start, end,
				    SplittableCompressionCodec.READ_MODE.BYBLOCK);
				in = new LineReader(cIn, job);
				start = cIn.getAdjustedStart();
				end = cIn.getAdjustedEnd();
				filePosition = cIn; // take pos from compressed stream
			  } else {
				in = new LineReader(codec.createInputStream(fileIn, decompressor), job);
				filePosition = fileIn;
			  }
			} else {
			  fileIn.seek(start);
			  in = new LineReader(fileIn, job);
			  filePosition = fileIn;
			}
			// If this is not the first split, we always throw away first record
			// because we always (except the last split) read one extra line in
			// next() method.
			if (start != 0) {
			  start += in.readLine(new Text(), 0, maxBytesToConsume(start));
			}
			this.pos = start;
		}

		
		private boolean isCompressedInput() 
		{
			return (codec != null);
		}

		private int maxBytesToConsume(long pos) 
		{
			return isCompressedInput() ? Integer.MAX_VALUE : (int) Math.min(Integer.MAX_VALUE, end - pos);
		}

		private long getFilePosition() throws IOException 
		{
			long retVal;
			if (isCompressedInput() && null != filePosition) 
			{
			  retVal = filePosition.getPos();
			} 
			else 
			{
			  retVal = pos;
			}
			return retVal;
		}

		public CustomerRecordReader(InputStream in, long offset, long endOffset, int maxLineLength) 
		{
			this.maxLineLength = maxLineLength;
			this.in = new LineReader(in);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;
			this.filePosition = null;
		}

		public CustomerRecordReader(InputStream in, long offset, long endOffset, Configuration job) throws IOException
		{
			this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
			this.in = new LineReader(in, job);
			this.start = offset;
			this.pos = offset;
			this.end = endOffset;    
			this.filePosition = null;
		}
		  
		public LongWritable createKey() 
		{
			return new LongWritable();
		}
		  
		public Text createValue() 
		{
			return new Text();
		}
		  
		/** Read content between '{' and '}'. */
		public synchronized boolean next(LongWritable key, Text value)throws IOException 
		{

			// We always read one extra line, which lies outside the upper
			// split limit i.e. (end - 1)
			Text lineString=new Text();
			String str="";
			int newSize=0;
			value.clear();
			while (getFilePosition() <= end) 
			{
			   key.set(pos);
			   newSize = in.readLine(lineString, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
			   if (newSize == 0) 
			   {
					return false;
			   }
			   pos += newSize;

			   str=lineString.toString();
			   while(str.contains("{")==false)
			   {
				   newSize = in.readLine(lineString, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
				   if (newSize == 0) 
				   {
						return false;
				   }
				   pos += newSize;
				   str=lineString.toString();
			   }

			   //if(str.contains("{")==true)
			   //{
			       str.replace("{","");
				   value.set(str.split(":")[1].replace("\r\n",""));
				   newSize = in.readLine(lineString);
				   pos += newSize;
				   str=lineString.toString();
				   while(str.contains("}")==false)
				   {
					   value.set(value.toString()+str.split(":")[1].replace("\r\n",""));
					   newSize = in.readLine(lineString, maxLineLength, Math.max(maxBytesToConsume(pos), maxLineLength));
					   if (newSize == 0) 
					   {
							return false;
					   }
					   pos += newSize;
					   str=lineString.toString();
				   }
				   //if(str.contains("}")==true)
				//}

			   if (newSize < maxLineLength) 
			   {
					return true;
			   }

			}

			return false;
		  }

		  /**
		   * Get the progress within the split
		   */
		public float getProgress() throws IOException 
		{
			if (start == end) 
			{
			  return 0.0f;
			} 
			else 
			{
			  return Math.min(1.0f,(getFilePosition() - start) / (float)(end - start));
			}
		}
		  
		public synchronized long getPos() throws IOException 
		{
			return pos;
		}

		public synchronized void close() throws IOException 
		{
			try 
			{
			  if (in != null) 
			  {
				in.close();
			  }
			} 
			finally 
			{
			  if (decompressor != null) 
			  {
				CodecPool.returnDecompressor(decompressor);
			  }
			}
		}  
	} 

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{
		 	      
	     public void map(LongWritable key, Text value, OutputCollector<Text,Text> output, Reporter reporter) throws IOException 
		 {

			 String line = value.toString();
	         String[] splits = line.split(",");
			 int count=splits.length;
		     float salary=Float.parseFloat(splits[3]);
		     if(salary > 1000)
			 {
		     	output.collect(value,new Text());
		     }	    			        
	     }
	}
	
		
    public static void main(String[] args) throws Exception 
	{
	      JobConf conf = new JobConf(problem2.class);
	      conf.setJobName("problem2");
	
	      conf.setOutputKeyClass(Text.class);
	      conf.setOutputValueClass(Text.class);
	
	      conf.setMapperClass(Map.class);
	      conf.setNumReduceTasks(0);
	
	      conf.setInputFormat(MyInputFormat.class);
	      conf.setOutputFormat(TextOutputFormat.class);
	
	      FileInputFormat.setInputPaths(conf, new Path(args[0]));
	      FileOutputFormat.setOutputPath(conf, new Path(args[1]));
	
	      JobClient.runJob(conf);
	}
}
		   


