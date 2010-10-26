package mappers;

import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.FastReadQuality;
import common.FastaRecordWritable;
import common.MultiMapResult;
import common.MultiMapResultPE;
import common.ReadInfoWritable;

class CloudMapper extends Mapper<IntWritable,FastaRecordWritable,Text,ReadInfoWritable>{
	//Input Key: file offset
	//Input value: one chunk of CHUNK_SIZE bytes
	//In an input Split: on record with many lines or many records with one line each?
	//Input split here base on blocks (64MB or 128MB) which is the default split?
	//output key: an integer/long indicate the position in the Vector best
	//output value: ReadInfo corresponding to that position in the output key

	private int seed_num = 3, seed_weight = 8, max_mismatches=0;
	private String filename = new String();		
	private int read_width = 0;	
	private int readperround=1000;
	private CloudAligner.INPUT_MODE mapperInputmode;
	private CloudAligner.MAP_MODE mapperMapmode;
	Configuration conf=null;


	protected void setup(Context con){			
		conf = con.getConfiguration();
		//read_width = conf.getInt("read_width", 0);//do we really need this?
		max_mismatches = conf.getInt("max_mismatches", 0);
		seed_num = conf.getInt("seed_num", 3);
		seed_weight = conf.getInt("seed_weight", 8);
		filename = conf.get("read_file");
		read_width = con.getConfiguration().getInt("read_width", 0);
//		mapping = conf.getInt("mapmode", 0);
		int informat = conf.getInt("informat", 0);	
		if (informat==0)
			mapperInputmode = CloudAligner.INPUT_MODE.FASTA_FILE;
		else mapperInputmode = CloudAligner.INPUT_MODE.FASTQ_FILE;
		int mapmode = conf.getInt("mapmode", 0);
		if (mapmode==0)
			mapperMapmode = CloudAligner.MAP_MODE.MAP;
		else if (mapmode==1)
			mapperMapmode = CloudAligner.MAP_MODE.BISULFITE;
		else mapperMapmode = CloudAligner.MAP_MODE.PAIR_END;
		System.out.println("Cloud Mapper. mapperMapmode = "+mapperMapmode);
		readperround = conf.getInt("readperround", 1000);
	}

	String ambiguous_file = null;
	String fasta_suffix = "fa";		
	public void map(IntWritable key, FastaRecordWritable value, Context context) throws IOException,InterruptedException{						
		byte [] seq = new byte[value.m_sequence.getLength()]; 
		System.arraycopy(value.m_sequence.getBytes(), 0, seq, 0, value.m_sequence.getLength());
		int chrom_id = value.m_offset.get();//this will be stored in the output data structure ("best")								
		//System.err.println("buffer = "+new String(buffer));
		try{
			Path thePath = new Path(filename);
			FileSystem fs = FileSystem.get(conf);
			SequenceFile.Reader theReader = null;				
			if (fs.exists(thePath)){
				theReader = new SequenceFile.Reader(fs, thePath, conf);
				assert(theReader!=null);					
				if (key.get()%2==0){//process the even partition
					//we need to do this to improve the throughput, 
					//each map deals with different blocks of the same block file
					//FIXME can do the thread here?					
					Text pseudokey = (Text)(theReader.getKeyClass().newInstance());
//					System.out.println("key = 1, before sync:"+theReader.getPosition());
					theReader.sync(theReader.getPosition()+10);//for the header man!!!!!!!! should be larger than the header size	
//					System.out.println("key = 1, after sync:"+theReader.getPosition());
					if (!theReader.next(pseudokey)) {
						IOUtils.closeStream(theReader);	
						return;//eof
					}
				}
//				System.out.println("key = "+key.toString());
				if (mapperMapmode == CloudAligner.MAP_MODE.BISULFITE)
					System.out.println("CloudMapper: In Bisulfite map mode");
				Aligner smap = new Aligner(mapperInputmode, CloudAligner.RUN_MODE.RUN_MODE_MISMATCH, mapperMapmode, true);										
				smap.initialize(read_width, max_mismatches, seed_num, seed_weight);
				boolean stop=false;
				int numread = 0;
				byte[] pseudo={-1};
				if (mapperMapmode == CloudAligner.MAP_MODE.PAIR_END){
					do{
						numread = smap.load_pe_reads(max_mismatches,readperround, theReader);
						if (numread < readperround)
							stop=true;
						if (numread > 0){
//							always load alternative blocks
							smap.execute(seq, chrom_id);						
							Vector<MultiMapResultPE> bests = smap.best_mapsPE;						
							double score=max_mismatches;
							for (int i = 0; i < bests.size(); ++i)
								if (!bests.elementAt(i).isEmpty()) {
									bests.elementAt(i).sort();
//									System.err.println("bests.elementAt(i).mr.size = "+bests.elementAt(i).mr.size());
									for (int j = 0; j < bests.elementAt(i).mr.size(); ++j)
										if (j == 0 || bests.elementAt(i).mr.elementAt(j - 1).isSmaller(bests.elementAt(i).mr.elementAt(j))) {
											final int left_start = bests.elementAt(i).mr.elementAt(j).strand ? 
													bests.elementAt(i).mr.elementAt(j).site : 
														seq.length - bests.elementAt(i).mr.elementAt(j).site2 - Aligner.read_width;
											final int right_start = bests.elementAt(i).mr.elementAt(j).strand ? 
													bests.elementAt(i).mr.elementAt(j).site2 : 
														seq.length - bests.elementAt(i).mr.elementAt(j).site - Aligner.read_width;
											score = (mapperInputmode==CloudAligner.INPUT_MODE.FASTQ_FILE)?
													FastReadQuality.value_to_quality(bests.elementAt(i).score):
														bests.elementAt(i).score;
											if (!smap.read_qualities.isEmpty()){
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))+"_L"), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																smap.read_qualities.elementAt(smap.read_index.elementAt(i)),//qualities of the read
																score,chrom_id,left_start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))+"_R"), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																smap.read_qualities.elementAt(smap.read_index.elementAt(i)),//qualities of the read
																score,chrom_id,right_start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
											}else{
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))+"_L"), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																pseudo,//null is not accepted here
																score,chrom_id,left_start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))+"_R"), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																pseudo,//null is not accepted here
																score,chrom_id,right_start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
											}
										}								
								}						
						}
					}  while (stop==false);
				} else {//BS or MAP mode
					do{				
						numread = smap.load_reads(max_mismatches,readperround, theReader);
						if (numread < readperround)
							stop=true;
						if (numread > 0){
							smap.execute(seq, chrom_id);
							Vector<MultiMapResult> bests = smap.best_maps;
							double score=max_mismatches;
							for (int i = 0; i < bests.size(); ++i)
								if (!bests.elementAt(i).isEmpty()) {
									bests.elementAt(i).sort();
//									System.err.println("bests.elementAt(i).mr.size = "+bests.elementAt(i).mr.size());
									for (int j = 0; j < bests.elementAt(i).mr.size(); ++j)
										if (j == 0 || bests.elementAt(i).mr.elementAt(j - 1).isSmaller(bests.elementAt(i).mr.elementAt(j))) {
											//int chrom_pos = bests.elementAt(i).mr.elementAt(j).chrom;
											//assert(chrom_id==chrom_pos);
											final int start = bests.elementAt(i).mr.elementAt(j).strand ? 
													bests.elementAt(i).mr.elementAt(j).site : 
														seq.length - bests.elementAt(i).mr.elementAt(j).site - Aligner.read_width;									
											score = (mapperInputmode==CloudAligner.INPUT_MODE.FASTQ_FILE)?
													FastReadQuality.value_to_quality(bests.elementAt(i).score):
														bests.elementAt(i).score;
//													System.out.println(smap.read_names.elementAt(smap.read_index.elementAt(i))+":"+bests.elementAt(i).score);													
											if (!smap.read_qualities.isEmpty()){
//														System.out.println("qual len="+smap.read_qualities.elementAt(smap.read_index.elementAt(i)).length);
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																smap.read_qualities.elementAt(smap.read_index.elementAt(i)),//qualities of the read
																score,chrom_id,start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
											}else 
												context.write(new Text(smap.read_names.elementAt(smap.read_index.elementAt(i))), //read id
														new ReadInfoWritable(
																smap.original_reads.elementAt(smap.read_index.elementAt(i)),//read content
																pseudo,//null is not accepted here
																score,chrom_id,start,bests.elementAt(i).mr.elementAt(j).strand,!bests.elementAt(i).ambiguous()));
										}									
								}
						}
					}  while (stop==false);
				}
				IOUtils.closeStream(theReader);	//finish one map, close the reader
			}else System.err.print("Read file not found!!");
		}catch (Exception e){
			e.printStackTrace();
		}
	}
}
