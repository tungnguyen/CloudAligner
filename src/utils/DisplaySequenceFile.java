package utils;
//Written by Alexander Mont <alexmont1@comcast.net>

import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DisplaySequenceFile {
	static StringBuilder sequence = new StringBuilder();
	static boolean print(SequenceFile.Reader theReader, int max) throws Exception{
		int numrecords=0;
		if (theReader.getValueClass() ==  BytesWritable.class)
		{
			Text key = (Text)(theReader.getKeyClass().newInstance());   
			BytesWritable value = new BytesWritable();
			boolean hasnext = false;
			while((numrecords<max)&&(hasnext = theReader.next(key,value)))
			{	 
				byte [] seq = new byte[value.getLength()]; 
				System.arraycopy(value.getBytes(), 0, seq, 0, value.getLength());
//				System.out.println(theReader.getPosition());
				for (int i=0;i<36;i++){
					System.out.print((char)seq[i]);
					sequence.append((char)seq[i]);
//					fw.write((char)seq[i]);	    			   
				}	    		   
//				fw.write("\n");
				System.out.println("");
				sequence.append('\n');
				numrecords++;
				if (theReader.syncSeen()){
					System.out.println("*");
					sequence.append("*\n");
					System.out.println("before seeing sync:"+theReader.getPosition());
					theReader.sync(theReader.getPosition()+10);	
					System.out.println("after seeing sync:"+theReader.getPosition());
					if (!theReader.next(key,value)) return false;//eof
				}
			}          			
			if ((numrecords<max)||!hasnext)
				return false;
			else return true;
		}
		else
		{
			Writable key = (Writable)(theReader.getKeyClass().newInstance());
			Writable value = (Writable)(theReader.getValueClass().newInstance());

			while(theReader.next(key,value))
			{
				System.out.println(key.toString() + " -> " + value.toString());
				numrecords++;
			}
		}
		return false;
	}
	public static void main(String[] args) throws Exception
	{
		String filename = null;
		//filename = "100k.br";

		if (filename==null){
			if (args.length != 1) {
				System.err.println("Usage: DisplaySequenceFile seqfile");
				System.exit(-1);
			}

			filename = args[0];
		}

		System.err.println("Printing " + filename);		
		Path thePath = new Path(filename);
		Configuration conf = new Configuration();

		SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
		int partition = 1;
		String outfile= "ref2.br";
		if (partition==1){
			outfile = "ref3.br";
			Text key = (Text)(theReader.getKeyClass().newInstance());  
			theReader.sync(300);//for the header man!!!!!!!! should be larger than the header size	
			System.out.println("after seeing sync:"+theReader.getPosition());
			if (!theReader.next(key)) return;//eof
//			else System.out.print(key.get());
		}
		while (print(theReader,10)){
			System.out.println();
		}
		FileWriter fw = new FileWriter(outfile);
		fw.write(sequence.toString());
		fw.close();
//		System.out.println("Saw " + numrecords);
	}
}
