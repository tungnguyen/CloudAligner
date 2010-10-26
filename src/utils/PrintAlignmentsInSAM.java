package utils;

import java.io.File;
import java.io.IOException;

import net.sf.samtools.Cigar;
import net.sf.samtools.CigarElement;
import net.sf.samtools.CigarOperator;
import net.sf.samtools.SAMFileHeader;
import net.sf.samtools.SAMFileWriter;
import net.sf.samtools.SAMFileWriterFactory;
import net.sf.samtools.SAMProgramRecord;
import net.sf.samtools.SAMRecord;
import net.sf.samtools.SAMSequenceRecord;
import net.sf.samtools.SAMUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import common.SAMRecordWritable;

public class PrintAlignmentsInSAM {
	
	private static Configuration conf = null;
//	private static AlignmentRecord ar = new AlignmentRecord();
	
	public static void printFile(Path thePath, String out, String chrom_name) throws IOException
	{
		SequenceFile.Reader theReader = new SequenceFile.Reader(FileSystem.get(conf), thePath, conf);
	       
	    Text key = new Text();
	    SAMRecordWritable value = new SAMRecordWritable();
	    File outputSamOrBamFile = new File(out);
	    String inputmode = "MISMATCH";
		int reflength = 10;		
		SAMFileHeader header = new SAMFileHeader();
		//@HD part
		header.setSortOrder(SAMFileHeader.SortOrder.unsorted);
		//@PG (program part)
		SAMProgramRecord progrec = new SAMProgramRecord("MIST");
		progrec.setProgramName("CloudAligner");
		progrec.setProgramVersion("0.1.2");
		progrec.setCommandLine(inputmode);
		header.addProgramRecord(progrec);
		//@SQ sequence part
		SAMSequenceRecord seqinfo = new SAMSequenceRecord(chrom_name,reflength);
		header.addSequence(seqinfo);
		SAMFileWriter outputSam = new SAMFileWriterFactory().makeSAMOrBAMWriter(header,
				false, outputSamOrBamFile);
	    
	    SAMRecord samRecord = new SAMRecord(header);
	    byte[] qual;
	    byte [] seq;
	    while(theReader.next(key,value))
	    {
//	    	System.out.println(value.toString());	    	
			samRecord.setReadUnmappedFlag(false);
			samRecord.setReadName(value.readID.toString());
			samRecord.setFlags(value.flag.get()); 
			samRecord.setReferenceName(chrom_name);
			samRecord.setAlignmentStart(value.lpos.get());
			samRecord.setMappingQuality(value.mapQ.get());			
			CigarElement ce = new CigarElement(value.sequence.getLength(), CigarOperator.M); 
			Cigar cigar = new Cigar();
			cigar.add(ce);
			samRecord.setCigar(cigar);
			
			samRecord.setMateReferenceName(SAMRecord.NO_ALIGNMENT_REFERENCE_NAME); //we can put "*" directly here?
			samRecord.setMateAlignmentStart(SAMRecord.NO_ALIGNMENT_START);//0
			samRecord.setInferredInsertSize(0);
			seq = new byte[value.sequence.getLength()]; 
			qual = new byte[value.quality.getLength()]; 
//			System.out.println(value.quality.getLength());
//			System.out.println(value.sequence.getLength());
			System.arraycopy(value.sequence.getBytes(), 0, seq, 0, value.sequence.getLength());
			System.arraycopy(value.quality.getBytes(), 0, qual, 0, value.quality.getLength());
			
			samRecord.setReadBases(seq);
			
			if (value.quality.getBytes()[0] != -1){
				//tricky here since Writable don't accept null while fasta files don't have qual scores				
				SAMUtils.fastqToPhred(qual);
				samRecord.setBaseQualities(qual);				
			}else samRecord.setBaseQualityString(SAMRecord.NULL_QUALS_STRING);
			//optional fields
			samRecord.setAttribute("PG", "CloudAligner");
//			samRecord.setAttribute("AS", 1);
			samRecord.setAttribute("NM", value.mismatchnum.get());
//			samRecord.setAttribute("MD", "25");
			outputSam.addAlignment(samRecord);			    	
	    }
	    outputSam.close();	
	}
	
	public static void main(String[] args) throws IOException 
	{
		String filename = null;
		//filename = "/user/guest/br-results/";
		String outfile = "results_cloud.sam";
		String chrname=null;
		if (filename == null)
		{
			if (args.length != 3) 
			{
				System.err.println("Usage: PrintAlignmentsInSAM seqfile chromname outfile");
				System.exit(-1);
			}
			filename = args[0];
			chrname = args[1];
			outfile = args[2];//this is supposed to be on local file system
		}
	
		
		System.err.println("Printing " + filename);
		
		Path thePath = new Path(filename);
	    //conf = new JobConf(AlignmentStats.class);
	    conf = new Configuration(); 
	    FileSystem fs = FileSystem.get(conf);
	       
	    if (!fs.exists(thePath))
	    {
	    	throw new IOException(thePath + " not found");   
	    }

	    FileStatus status = fs.getFileStatus(thePath);

	    if (status.isDir())
	    {    	   
	    	FileStatus [] files = fs.listStatus(thePath);
	    	for(FileStatus file : files)
	    	{
	    		String str = file.getPath().getName();

	    		if (str.startsWith("."))
	    		{
	    			// skip
	    		}			   
	    		else if (!file.isDir())
	    		{
	    			printFile(file.getPath(),outfile,chrname);
	    		}
	    	}
	    }
	    else
	    {
	    	printFile(thePath,outfile,chrname);	   
	    }
	}
}

