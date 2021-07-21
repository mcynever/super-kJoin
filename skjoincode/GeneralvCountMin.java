package skjoincode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            

public class GeneralvCountMin {
public static Random rand = new Random();
	
	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static int M = 1024 * 1024 * 16; 	// total memory space Mbits
	public static GeneralDataStructure[] C;
	public static GeneralDataStructure D;
	public static GeneralDataStructure[] B;
	//public static Set<Integer> sizeMeasurementConfig = new HashSet<>(Arrays.asList()); // 0-counter; 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static Set<Integer> spreadMeasurementConfig = new HashSet<>(Arrays.asList(2)); // 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static boolean isGetThroughput =false; 
	
	/** parameters for sharing approach **/
	public static int d = 4;
	public static int u = 1;			// the unit size of each virtual data structure
	public static int w;				// number of the unit data structures in the physical array
	public static int m = 1;			// useless number of elementary data structures in the virtual data structure
	public static int[] S; 				// random seeds for the sharing approach
	public static int[] SS; 				// random seeds for the sharing approach
	
	/** parameters for counters **/
	public static int mValueCounter =1;
	public static int counterSize = 20;

	/** parameters for bitmap **/
	public static int bitmapSize = 1;	// sharing at bit level
	public static int virtualArrayLength = 5000;
	
	/** parameters for FM sketch **/
	public static int mValueFM = 128;
	public static int FMsketchSize = 32;
	
	/** parameters for hyperLogLog **/
	public static int mValueHLL = 128;
	public static int HLLSize = 5;
	
	public static int times = 1;
	
	/** number of runs for throughput measurement */
	public static int loops = 10;
	public static int Mbase=1024*1024;
	public static int[] segmentNumarray = {1};
	public static int segmentLength = 1;
	public static int[][] Marray= {{2},{2}};
	public static int[][]	mValueCounterarray= {{1},{32}};
	public static int[][] virtualArrayLengtharray= {{50000},{4992}};
	public static int[][] mValueFMarray= {{64},{128}};
	public static int[][] mValueHLLarray= {{128},{128}};
	public static long c1=0;
	public static long c2=0;
	
	

	public static int maxWorst = 0,minWorst = Integer.MAX_VALUE,average = 0;



	
	public static void main(String[] args) throws FileNotFoundException {
		/** measurment for flow sizes **/
		if (isGetThroughput) {
			getThroughput();
			return;
		}
		System.out.println("Start************************");
		Random random=new Random();
		c1=random.nextLong();
		c2=random.nextLong();
		/** measurment for flow sizes **/
		
		
		
		/** measurment for flow spreads **/
		for (int i : spreadMeasurementConfig) {
			for(int i1=0;i1<Marray[1].length;i1++) {
				for (int i2 = 0;i2<segmentNumarray.length;i2++) {
					maxWorst = 0;
					minWorst = Integer.MAX_VALUE;
					average = 0;
					
				for (int i3 = 0;i3<1;i3++) {
					String inputfile = "\\D:\\GeneralFramework\\CAIDA\\output"+Integer.toString(i3)+"v.txt";
					String outputfile = "\\D:\\GeneralFramework\\CAIDA\\dstSpread" +Integer.toString(i3)+"v.txt";

					M=Marray[1][i1]*Mbase;
				switch (i) {
				case 0:
					   for(int j=0;j<mValueCounterarray[0].length;j++) {
						 

						   mValueCounter=mValueCounterarray[0][j];
						   initSharing(i);
						   initJoining(i);
						   initUnioning(i);
							encodeSpread(GeneralUtil.dataStreamForFlowSpread);
				        	estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
					   }
					   break;
				case 1:
					for(int j=0;j<virtualArrayLengtharray[1].length;j++) {
						
						virtualArrayLength=virtualArrayLengtharray[1][j];
						
						segmentLength = virtualArrayLength / segmentNumarray[i2];
						initSharing(i);
						initJoining(i);
						initUnioning(i);
						encodeSpread(GeneralUtil.dataStreamForFlowSpread);
			    		estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
					}
					break;
				case 2:	
					for(int j=0;j<mValueFMarray[1].length;j++) {
					
						mValueFM=mValueFMarray[1][j];
						segmentLength = mValueFM / segmentNumarray[i2];

						initSharing(i);
						initJoining(i);
						initUnioning(i);
						encodeSpread(GeneralUtil.dataStreamForFlowSpread);
			    		estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
					}
					break;
				case 3:
					for(int j=0;j<mValueHLLarray[1].length;j++) {
					
						mValueHLL=mValueHLLarray[1][j];
						segmentLength = mValueHLL / segmentNumarray[i2];

						initSharing(i);
						initJoining(i);
						initUnioning(i);
						encodeSpread(inputfile);
			    		estimateSpread(outputfile);
						//encodeSpread(GeneralUtil.dataStreamForFlowSpread);
			    		//estimateSpread(GeneralUtil.dataSummaryForFlowSpread);
					}
					break;
				default:break;
				}
				}
				String resultFilePath = "D:\\GeneralFramework\\vCOuntMinNewDesign\\spread\\" + "maxmaxmax_estimator_"+i+"_segmentlength_"+segmentLength;
				PrintWriter pw = new PrintWriter(new File(resultFilePath));
				System.out.println("estimator:"+i+"\t" + "segmentlength:"+segmentLength+"\t"+average/10 +"\t"+minWorst+"\t"+maxWorst);
				pw.println("estimator:"+i+"\t" + "segmentlength:"+segmentLength+"\t"+average/10 +"\t"+minWorst+"\t"+maxWorst);
				pw.close();
			}
			}
		}
		System.out.println("DONE!!!!!!!!!!!");
	}
	
	
	
	// Init the VSketch approach for different elementary data structures.
	public static void initSharing(int index) {
		switch (index) {
	        case 0:  C = generateCounter(); 
	        		// D = generateCounter(); 
	                 break;
	        case 1:  C = generateBitmap(); 
	        		// D = generateBitmap();
	                 break;
	        case 2:  C = generateFMsketch(); 
	                // D = generateFMsketch();
	                 break;
	        case 3:  C = generateHyperLogLog();
	        		 //D = generateHyperLogLog();
	                 break;
	        default: break;
		}
		generateSharingRandomSeeds();
		System.out.println(index);
		System.out.println("\n the " + C[0].getDataStructureName() + " Initialized!");
	}
	
	public static void initJoining(int index) {
		switch (index) {
	        case 0:  B = new Counter[1]; B[0]=new Counter(mValueCounter,counterSize); 
	                 break;
	        case 1:  B = new Bitmap[1]; B[0] = new Bitmap(virtualArrayLength);
	                 break;
	        case 2:  B = new FMsketch[1]; B[0] = new FMsketch(mValueFM, FMsketchSize);
	                 break;
	        case 3:  B = new HyperLogLog[1]; B[0] = new HyperLogLog(mValueHLL, HLLSize);
	                 break;
	        default: break;
		}
		//generateSharingRandomSeeds();
		System.out.println("\nVCM-" + C[0].getDataStructureName() + " Initialized!");
	}
	
	public static void initUnioning(int index) {
		System.out.println(mValueHLL);
		switch (index) {
		case 0:  D = new Counter(mValueCounter,counterSize); 
		break;
		case 1:  D = new Bitmap(virtualArrayLength);
		break;
		case 2:  D = new FMsketch(mValueFM, FMsketchSize);
		break;
		case 3:  D = new HyperLogLog(mValueHLL, HLLSize);
		break;
		default: break;
		}
	}
	
	// Generate counter sharing data structures for flow size measurement.
	public static Counter[] generateCounter() {
		m = mValueCounter;		
		//System.out.println(m);

		u = counterSize;
		w = M / u / d;
		System.out.println(w);
		Counter[] B = new Counter[d];
		for(int i = 0 ; i<d ; i++ )
			B[i] = new Counter(w, counterSize);
		return B;
	}
		
	// Generate bit sharing data structures for flow cardinality measurement.
	public static Bitmap[] generateBitmap() {
		
		m = virtualArrayLength;
		u = bitmapSize;
		w =( M / u / d /segmentLength)*segmentLength;
		Bitmap[] B = new Bitmap[d];
		for(int i = 0 ; i<d ; i++ )
			B[i] = new Bitmap(w);
		return B;
	}
	
	// Generate FM sketch sharing data structures for flow cardinality measurement.
	public static FMsketch[] generateFMsketch() {
		m = mValueFM;
		u = FMsketchSize;
		w =( M / u / d /segmentLength)*segmentLength;
		System.out.println("w"+"\t"+w);
		FMsketch[] B = new FMsketch[d];
		for(int i = 0 ; i<d ; i++ )
			B[i] = new FMsketch(w, FMsketchSize);
		return B;
	}
	
	// Generate register sharing data structures for flow cardinality measurement.
	public static HyperLogLog[] generateHyperLogLog() {
		m = mValueHLL;
		u = HLLSize;
		w =( M / u / d /segmentLength)*segmentLength;
		HyperLogLog[] B = new HyperLogLog[d];
		for(int i = 0 ; i<d ; i++ )
			B[i] = new HyperLogLog(w, HLLSize);
		return B;
	}
	
	// Generate random seeds for Sharing approach.
	public static void generateSharingRandomSeeds() {
		HashSet<Integer> seeds = new HashSet<Integer>();
		S = new int[m];
		int num = m;
		while (num > 0) {
			int s = rand.nextInt();
			if (!seeds.contains(s)) {
				num--;
				S[num] = s;
				seeds.add(s);
			}
		}
		seeds = new HashSet<Integer>();
		SS =new int[d];
		num=d;
		while (num > 0) {
			int s = rand.nextInt();
			if (!seeds.contains(s)) {
				num--;
				SS[num] = s;
				seeds.add(s);
			}
		}
	}
	
	/** Encode elements to the physical data structure for flow size measurement. */
	
	/** Encode elements to the physical data structure for flow spread measurement. */
	public static void encodeSpread(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements using " + C[0].getDataStructureName().toUpperCase() + "s for flow spread measurement..." );
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		int nSpread =0;
	//	int nnn=0;
		//long startTime = System.nanoTime();
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String[] res = GeneralUtil.getSperadFlowIDAndElementID(strs, true);
			//String flowid = res[0];
			//String elementid = res[1];
			long flowid = Long.parseLong(res[0]);
			long elementid = Long.parseLong(res[1]);
			n++;
			int min_value = Integer.MAX_VALUE;
			int[] temp = new int[d];
    		//if (n%1000000==0)System.out.println("Total befroe number of encoded pakcets: " + n);

			
					for(int i = 0 ; i < d ; i++) {
						C[i].encodevCMSegment(flowid, elementid , S, SS[i] , w , segmentLength);


					}
				
						
			
		}
		System.out.println("Total number of encoded pakcets: " + n); 
		//long endTime = System.nanoTime();
		//System.out.println("encodespeed: " + (double)nnn/(endTime-startTime)*1000000000.0);
		sc.close();
	}
	
	
	/** Estimate flow spreads. */
	public static void estimateSpread(String filePath) throws FileNotFoundException {
		System.out.println("Estimating Flow CARDINALITY..." ); 
		Scanner sc = new Scanner(new File(filePath));
		String resultFilePath = "D:\\GeneralFramework\\vCOuntMinNewDesign\\spread\\" + C[0].getDataStructureName()
				+"segment" +segmentLength+ "_M_" +  M / 1024 / 1024 + "_u_" + u + "_m_" + m;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		int noise=0;
		int[] te=new int[w];
		for(int i=0;i<w;i++)
			noise+=D.getspreadnoise(C).getValue();
		noise/=w;
		//noise=Integer.MAX_VALUE;
		/*/for (int t = 0; t < m; t++) {
			B[0] = B[0].join(C[0],w/m,t);
		}/*/
		//C[0]
		System.out.println("Result directory: " + resultFilePath); 
		//int totalSum1=HLLArray[0].getValue();
		//int totalSum1=B[0].getValue();
		// Get the estimate of the physical data structure.
		//int totalSum = C[0].getValue();
		//if(C[0].getDataStructureName().equals("Bitmap")) {
		//	totalSum1=totalSum;
		//}
		//totalSum1=totalSum;
		//System.out.println("total spreads1: " + totalSum1);
		//System.out.println("total spreads: " + totalSum);
		//n=0;
		//Double threshold = Math.sqrt(1.0 * totalSum1 * (1.0 - 1.0 / (w / m)) / (w / m));
		//System.out.println(threshold);
		//threshold=0.0;
		//long startTime = System.nanoTime();
		System.out.println(noise);
		//noise=137;
		int worst = 0;
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			//String flowid = GeneralUtil.getSperadFlowIDAndElementID(strs, false)[0];
			long flowid = Long.parseLong(GeneralUtil.getSperadFlowIDAndElementID(strs, false)[0]);
			int actualSpread = Integer.parseInt(GeneralUtil.getSperadFlowIDAndElementID(strs, false)[1]);
			int num = Integer.parseInt(strs[strs.length-1]);
			n+=num;
			// Get the estimate of the virtual data structure.
			int virtualSum,estimate=Integer.MAX_VALUE;	
			int min_value =Integer.MAX_VALUE;
			int[] temp = new int[d];
			if (!C[0].getDataStructureName().equals("Counter")) {///  <--------------- this what we get into
				 virtualSum = D.getvCMminValueSegment(C, flowid, S, SS, w, segmentLength).getValue();
					estimate=virtualSum;	

			}
			else {///<------------------- this is not used
				for(int i = 0 ; i < d ; i++) {
					int tempSum = 0;
					for  (int j = 0;j<S.length;j++) {
						int i0 = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[j] ^ SS[i]) % w + w) % w;
		    			tempSum = tempSum +C[i].getCounters()[i0];
        			
        		}
				min_value = Math.min(tempSum, min_value); 
				estimate = min_value;
			}
			}

			//if (estimate < noise*2)
				//estimate-=noise;
			//Double estimate = Math.max(1.0 * (virtualSum - 1.0 * m * totalSum1 / w), 1);
			//threshold=0.0;
			//System.out.println(threshold);
			//int sample_ratio = 16;
			//if(estimate)
			if (estimate < 1) {
				//virtualSum = C[0].getOptValueSegment(flowid, S, w / m, sample_ratio);
				//estimate = Math.max(1.0 * (virtualSum - 1.0 * m * totalSum / w/sample_ratio), 1);
				estimate=1;
			}
			
			estimate = (int) estimate;
			pw.println(entry + "\t" + estimate);
			if (worst<estimate-actualSpread) worst = estimate-actualSpread;
		}
		//long endTime = System.nanoTime();
		//System.out.println("qspeed: " + (double)rr/(endTime-startTime)*1000000000.0);
		sc.close();
		pw.close();
		average += worst;
		if (worst>maxWorst) maxWorst = worst;
		if (worst<minWorst) minWorst = worst;
		// obtain estimation accuracy results
		System.out.println("total flow spread="+n);
		GeneralUtil.analyzeAccuracy(resultFilePath, " ", 1);
	}
	
	public static void getThroughput() throws FileNotFoundException {
		Scanner sc = new Scanner(new File(GeneralUtil.dataStreamForFlowSpread));
		int x=0;
		ArrayList<Long> dataFlowID = new ArrayList<Long> ();
		ArrayList<Long> dataElemID = new ArrayList<Long> ();
		n = 0;
		
		
			while (sc.hasNextLine()) {
				String entry = sc.nextLine();				
				String[] strs = entry.split("\\s+");
				dataFlowID.add(Long.parseLong(strs[1]));
				dataElemID.add(Long.parseLong(strs[0]));
				n++;
			}
			sc.close();
		
		System.out.println("total number of packets: " + n);
		
		/** measurement for flow sizes **/
		
		/** measurement for flow spreads **/
		for (int i : spreadMeasurementConfig) {
			tpForSpread(i, dataFlowID, dataElemID);
		}
	}
	
	/** Get throughput for flow size measurement. */
	
	
	/** Get throughput for flow spread measurement. */
	public static void tpForSpread(int vSketch, ArrayList<Long> dataFlowID, ArrayList<Long> dataElemID) throws FileNotFoundException {
		int totalNum = dataFlowID.size();
		initSharing(vSketch);
		String resultFilePath = "D:\\GeneralFramework\\Throughput\\" + C[0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_u_" + u + "_m_" + m;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		Double res = 0.0;
		System.out.println("begin throughput for spread");
		double duration = 0;
		int nSpread = 0;
		int w_m = w / m;
		for (int i = 0; i < loops; i++) {
			

			   initSharing(vSketch);
			   initJoining(vSketch);
			   initUnioning(vSketch);
			//long startTime = System.nanoTime();
			for (int j = 0; j < totalNum; j++) {
				int min_value = Integer.MAX_VALUE;
				int[] temp = new int[d];
				
	    		

				
					nSpread+=1;
					if (C[0].getDataStructureName().equals("Counter")) {
		        		for(int ii = 0 ; ii < d ; ii++) {
		        			int i0 = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[0] ^ SS[ii]) % w + w) % w;
		        			temp[ii] =i0;
		        			min_value = Math.min(C[ii].getCounters()[temp[ii]], min_value); 		        			
		        		}			        
		        		for(int ii = 0 ; ii < d ; ii++) {
		        			if (C[ii].getCounters()[temp[ii]]==min_value) C[ii].getCounters()[temp[ii]]++;
		        		}		        		
		        	}
					else {
						for(int ii = 0 ; ii < d ; ii++) {
							C[ii].encodevCMSegment(dataFlowID.get(j), dataElemID.get(j) , S, SS[ii] , w, segmentLength);
						}					
					}	
				}
			
			//long endTime = System.nanoTime();
			totalNum = 1000;
			long startTime = System.nanoTime();
			for (int j = 0; j < totalNum; j++) {
				int min_value = Integer.MAX_VALUE;
				int[] temp = new int[d];
				
	    		

				
					nSpread+=1;
					if (C[0].getDataStructureName().equals("Counter")) {
		        		for(int ii = 0 ; ii < d ; ii++) {
		        			int i0 = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[0] ^ SS[ii]) % w + w) % w;
		        			temp[ii] =i0;
		        			min_value = Math.min(C[ii].getCounters()[temp[ii]], min_value); 		        			
		        		}			        
		        		for(int ii = 0 ; ii < d ; ii++) {
		        			if (C[ii].getCounters()[temp[ii]]==min_value) C[ii].getCounters()[temp[ii]]++;
		        		}		        		
		        	}
					else {
						 int virtualSum = D.getvCMminValueSegment(C, dataFlowID.get(j), S, SS, w,segmentLength).getValue();
							int estimate=virtualSum;	
				
					}	
				}
			
			long endTime = System.nanoTime();
			duration += 1.0 * (endTime - startTime) / 1000000000;
		}
		
		res = 1.0 * totalNum / (duration / loops);
		//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
		System.out.println(C[0].getDataStructureName() + "\t Average Throughput: " + 1.0 * totalNum / (duration / loops) + " packets/second" );
		
		pw.println(res.intValue());
		pw.close();
	}
}
