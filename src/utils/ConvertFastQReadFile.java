package utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import common.ReadRecordWritable;

public class ConvertFastQReadFile {
	// process fastq input files
	static boolean	is_fastq_name_line(long line_count) {
		return ((line_count & 3l) == 0l);
	}

	static boolean	is_fastq_sequence_line(long line_count) {
		return ((line_count & 3l) == 1l);
	}

	static boolean	is_fastq_score_name_line(long line_count) {
		return ((line_count & 3l) == 2l);
	}

	static boolean	is_fastq_score_line(long line_count) {
		return ((line_count & 3l) == 3l);
	}
	
	public static byte [] stringToBytes(String src)
	{
		int srclen = src.length();
		byte [] ret = new byte[srclen];
		
		for (int i = 0; i < srclen; i++)
		{
			ret[i] = (byte) src.charAt(i);
		}
		
		return ret;
	}
	public static byte [] scoresToBytes(String src)
	{
		//Phred or Solexa/Illumina quality?
		int srclen = src.length();
		byte [] ret = new byte[srclen];
		
		for (int i = 0; i < srclen; i++)
		{
			ret[i] = (byte) (src.charAt(i) - 33);//Phred
		}
		
		return ret;
	}
	private static Text key = new Text();

	public static void convertFile(String infile,String outfile) throws IOException
	{
		int read_count = 0;	
		try{
			Configuration config = new Configuration();			
			SequenceFile.Writer writer = SequenceFile.createWriter(FileSystem.get(config), config,
					new Path(outfile), Text.class, ReadRecordWritable.class);			

			BufferedReader inputStream = null;		
			inputStream = new BufferedReader(new FileReader(infile));			
			String l,seq=null,scores = null,name = null;			
			long line_count = 0;
//			FIXME: we should ensure the format of the fastq here
			while ((l = inputStream.readLine()) != null) {
				//System.out.println(l);		
				if (is_fastq_sequence_line(line_count)) {		
					seq = new String(l); 
					++read_count;
				} else if (is_fastq_name_line(line_count))
					name = new String(l);
				else if (is_fastq_score_line(line_count)){//the last line
					scores = new String(l);
					key.set(name);
					writer.append(key, new ReadRecordWritable(stringToBytes(seq),stringToBytes(scores)));
					//we don't care about the quality scores in this version so we use stringToBytes too for scores
				}
				line_count++;				
			}			
			writer.sync();
			writer.close();	
		} 
		catch (FileNotFoundException e) 
		{
			System.err.println("Can't open " + infile);
			e.printStackTrace();
			System.exit(1);
		}		
		System.err.println("Processed " + read_count + " sequences");
	}

	public static void main(String[] args) throws IOException 
	{
		if (args.length < 2) {
			System.err.println("Usage: ConvertFastQReadFile infile outfile");
			System.exit(-1);
		}		
		String infile = args[0];
		String outfile = args[1];
		convertFile(infile,outfile);		
	}
}