package kjoincode;
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


/** A general framework for count min. The elementary data structures to be shared here can be counter, bitmap, FM sketch, HLL sketch. Specifically, we can
 * use counter to estimate flow sizes, and use bitmap, FM sketch and HLL sketch to estimate flow cardinalities
 * @author Jay, Youlin, 2018. 
 */

public class GeneralCountMinbitwise {
	public static Random rand = new Random();
	
	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static final int M = 1024 * 1024 * 2; 	// total memory space Mbits	
	public static GeneralDataStructure[][] C;
	public static GeneralDataStructure D;

	//public static Set<Integer> sizeMeasurementConfig = new HashSet<>(Arrays.asList()); // -1-regular CM; 0-enhanced CM; 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static Set<Integer> spreadMeasurementConfig = new HashSet<>(Arrays.asList(2)); // 1-Bitmap; 2-FM sketch; 3-HLL sketch
	public static Set<Integer> expConfig = new HashSet<>(Arrays.asList()); //0-ECountMin dist exp
	public static boolean isGetThroughput = false;
	
	/** parameters for count-min */
	public static final int d = 4; 			// the number of rows in Count Min
	public static int w = 1;				// the number of columns in Count Min
	public static int u = 1;				// the size of each elementary data structure in Count Min.
	public static int[] S = new int[d];		// random seeds for Count Min
	public static int m = 1;				// number of bit/register in each unit (used for bitmap, FM sketch and HLL sketch)

	
	/** parameters for counter */
	public static int mValueCounter = 1;			// only one counter in the counter data structure
	public static int counterSize = 20;				// size of each unit

	/** parameters for bitmap */
	public static final int bitArrayLength = 5000;
	
	/** parameters for FM sketch **/
	public static int mValueFM = 128;
	public static final int FMsketchSize = 32;
	
	/** parameters for HLL sketch **/
	public static int mValueHLL = 128;
	public static final int HLLSize = 5;

	public static int times = 0;
	
	/** number of runs for throughput measurement */
	public static int loops = 100;
	public static int dataFileNumber = 1;

	public static void main(String[] args) throws FileNotFoundException {
		/** measurement for flow sizes **/
		if (isGetThroughput) {
			getThroughput();
			return;
		}
		System.out.println("Start****************************");
		/** measurement for flow sizes **/
	
		for (int ii=0;ii<dataFileNumber;ii++) {	

		/** measurement for flow spreads **/
		for (int i : spreadMeasurementConfig) {
			initCM(i);
			initUnioning(i);

			encodeSpread(GeneralUtil.dataStreamForFlowSpreadJoin+GeneralUtil.fileSuffix[ii]+".txt");
    		estimateSpread(GeneralUtil.dataSummaryForFlowSpreadJoin+GeneralUtil.fileSuffix[ii]+".txt", Integer.toString(ii));
    		}
		}
		/** experiment for specific requirement *
		for (int i : expConfig) {
			switch (i) {
	        case 0:  initCM(0);
					 encodeSize(GeneralUtil.dataStreamForFlowSize);
					 randomEstimate(10000000);
	                 break;
	        default: break;
			}
		}*/
		System.out.println("DONE!****************************");
	}
	
	// Init the Count Min for different elementary data structures.
	public static void initCM(int index) {
		switch (index) {
	        case 0: case -1: C = generateCounter();
	                 break;
	        case 1:  C = generateBitmap();
	                 break;
	        case 2:  C = generateFMsketch();
	                 break;
	        case 3:  C = generateHyperLogLog();
	                 break;
	        default: break;
		}
		generateCMRamdonSeeds();
		System.out.println("\nCount Min-" + C[0][0].getDataStructureName() + " Initialized!");
	}
	public static void initUnioning(int index) {
		System.out.println(mValueHLL);
		switch (index) {
		case 0:  D = new Counter(mValueCounter,counterSize); 
		break;
		case 1:  D = new Bitmap(bitArrayLength);
		break;
		case 2:  D = new FMsketch(mValueFM, FMsketchSize);
		break;
		case 3:  D = new HyperLogLog(mValueHLL, HLLSize);
		break;
		default: break;
		}
	}
	// Generate counter base Counter Min for flow size measurement.
	public static Counter[][] generateCounter() {
		m = mValueCounter;
		u = counterSize * mValueCounter;
		w = (M / u) / d;
		Counter[][] B = new Counter[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new Counter(1, counterSize);
			}
		}
		return B;
	}
	
	// Generate bitmap base Counter Min for flow cardinality measurement.
	public static Bitmap[][] generateBitmap() {
		m = bitArrayLength;
		u = bitArrayLength;
		w = (M / u) / d;
		Bitmap[][] B = new Bitmap[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new Bitmap(bitArrayLength);
			}
		}
		return B;
	}
	
	// Generate FM sketch base Counter Min for flow cardinality measurement.
	public static FMsketch[][] generateFMsketch() {
		m = mValueFM;
		u = FMsketchSize * mValueFM;
		w = (M / u) / d;
		FMsketch[][] B = new FMsketch[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new FMsketch(mValueFM, FMsketchSize);
			}
		}
		return B;
	}
	
	// Generate HLL sketch base Counter Min for flow cardinality measurement.
	public static HyperLogLog[][] generateHyperLogLog() {
		m = mValueHLL;
		u = HLLSize * mValueHLL;
		w = (M / u) / d;
		HyperLogLog[][] B = new HyperLogLog[d][w];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < w; j++) {
				B[i][j] = new HyperLogLog(mValueHLL, HLLSize);
			}
		}
		return B;
	}
	
	// Generate random seeds for Counter Min.
	public static void generateCMRamdonSeeds() {
		HashSet<Integer> seeds = new HashSet<Integer>();
		int num = d;
		while (num > 0) {
			int s = rand.nextInt();
			if (!seeds.contains(s)) {
				num--;
				S[num] = s;
				seeds.add(s);
			}
		}
	}

	/** Encode elements to the Count Min for flow size measurement. */
	
	/** Encode elements to the Count Min for flow spread measurement. */
	public static void encodeSpread(String filePath) throws FileNotFoundException {
		System.out.println("Encoding elements using " + C[0][0].getDataStructureName().toUpperCase() + "s..." );
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String[] res = GeneralUtil.getSperadFlowIDAndElementID(strs, true);
			String flowid = res[0];
			String elementid = res[1];
			n++;
			for (int i = 0; i < d; i++) {
				int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[i]) % w + w) % w;
				C[i][j].encode(elementid);
			}
		}
		System.out.println("Total number of encoded pakcets: " + n); 
		sc.close();
	}
	public static int getBitSetValue(BitSet b) {
		int res = 0;
		for (int i = 0; i < b.length(); i++) {
			if (b.get(i)) res += Math.pow(2, i);
		}
		return res;
	}
	public static void setBitSetValue(BitSet b, int value) {
		int i = 0;
		while (value != 0 && i < HLLSize) {
			if ((value & 1) != 0) {
				b.set(i);
			} else {
				b.clear(i);
			}
			value = value >>> 1;
			i++;
		}
	}
	/** Estimate flow spreads. */
	public static void estimateSpread(String filepath,String ii) throws FileNotFoundException {
		System.out.println("Estimating Flow CARDINALITY..." ); 
		Scanner sc = new Scanner(new File(filepath));
		String filePathPrefix = "D:\\GeneralFramework\\bitwiseCM\\spread\\";
		String filePathSuffix = C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m;
		String resultFilePath = filePathPrefix +ii+"v"+filePathSuffix;

		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		System.out.println("Result directory: " + resultFilePath); 
		while (true&&sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String flowid = GeneralUtil.getSperadFlowIDAndElementID(strs, false)[0];
			int num = Integer.parseInt(strs[strs.length-1]);
			// TODO(youzhou): Add sampling mechanism to reduce the computation time.
			if (rand.nextDouble() <= GeneralUtil.getSpreadSampleRate(num)) {
				int estimate = Integer.MAX_VALUE;
				int temp = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[0]) % w + w) % w;
				if (C[0][0].getDataStructureName().equals("Bitmap")) {
				for (int k=0;k<D.getUnitSize();k++)	{
					if (C[0][temp].getBitmaps().get(k)) {
						D.getBitmaps().set(k);
					}
					else
						D.getBitmaps().clear(k);
				}
				for(int i = 1; i < d; i++) {
					int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[i]) % w + w) % w;
					for (int k=0;k<C[0][temp].getUnitSize();k++)	{
						if (!C[i][j].getBitmaps().get(k)) {
							D.getBitmaps().clear(k);
						}
					}
				}
				estimate=D.getValue();
				}
				if (C[0][0].getDataStructureName().equals("FMsketch")) {
					for (int k=0;k<mValueFM ;k++)	{
						for(int l=0;l<FMsketchSize;l++)
						if (C[0][temp].getFMSketches()[k].get(l)) {
							D.getFMSketches()[k].set(l);
						}
						else
							D.getFMSketches()[k].clear(l);
					}
					for(int i = 1; i < d; i++) {
						int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[i]) % w + w) % w;
						for (int k=0;k<mValueFM ;k++)	{
							for(int l=0;l<FMsketchSize;l++)
								if (!C[i][j].getFMSketches()[k].get(l)) {	
									D.getFMSketches()[k].clear(l);
							}
						}
					}
					estimate=D.getValue();
				}
				if (C[0][0].getDataStructureName().equals("HyperLogLog")) {
					for (int k=0;k<mValueHLL ;k++)	{
						//for(int l=0;l<HLLSize;l++)
						D.getHLLs()[k] = C[0][temp].getHLLs()[k];
						//if (C[0][temp].getHLLs()[k].get(l)) {
						//	D.getHLLs()[k].set(l);
						//}
						//else
							//D.getHLLs()[k].clear(l);
					}
					for(int i = 1; i < d; i++) {
						int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1((flowid)) ^ S[i]) % w + w) % w;
						for (int k=0;k<mValueHLL;k++)	{
								if (getBitSetValue(C[i][j].getHLLs()[k])<getBitSetValue(D.getHLLs()[k])) {	
									D.getHLLs()[k]=C[i][j].getHLLs()[k];
									//D.setBitSetValue(k,getBitSetValue(C[i][j].getHLLs()[k]));
									if (getBitSetValue(C[i][j].getHLLs()[k])!=getBitSetValue(D.getHLLs()[k])) { 
										System.out.println("error--------------"+getBitSetValue(C[i][j].getHLLs()[k])+"  "+getBitSetValue(D.getHLLs()[k]));
										System.out.println(D.getHLLs()[k]);
										System.out.println(C[i][j].getHLLs()[k]);

									}
								}
						}
					}
					estimate=D.getValue();
				}
				pw.println(entry + "\t" + estimate);
			}
		}
		sc.close();
		pw.close();
		// obtain estimation accuracy results
		GeneralUtil.analyzeAccuracy(resultFilePath);
		//if (Integer.parseInt(ii)+1==dataFileNumber) {
		//	GeneralUtil.analyzeAccuracy(filePathPrefix,filePathSuffix,dataFileNumber);
		//}
	}
	
	public static void getThroughput() throws FileNotFoundException {
		Scanner sc = new Scanner(new File(GeneralUtil.dataStreamForFlowThroughput));
		ArrayList<Integer> dataFlowID = new ArrayList<Integer> ();
		ArrayList<Integer> dataElemID = new ArrayList<Integer> ();
		n = 0;
		
			while (sc.hasNextLine()) {
				String entry = sc.nextLine();				
				String[] strs = entry.split("\\s+");
				dataFlowID.add(GeneralUtil.FNVHash1(strs[0]));
				dataElemID.add(GeneralUtil.FNVHash1(strs[1]));
				n++;
			}
			sc.close();
		
		System.out.println("total number of packets: " + n);
		
		/** measurment for flow sizes **/
	
		/** measurment for flow spreads **/
		for (int i : spreadMeasurementConfig) {
			tpForSpread(i, dataFlowID, dataElemID);
		}
	}
	
	/** Get throughput for flow size measurement. */
	
	/** Get throughput for flow spread measurement. */
	public static void tpForSpread(int sketchMin, ArrayList<Integer> dataFlowID, ArrayList<Integer> dataElemID) throws FileNotFoundException {
		int totalNum = dataFlowID.size();
		initCM(sketchMin);
		String resultFilePath = GeneralUtil.path + "Throughput\\CM_spread_" + C[0][0].getDataStructureName()
				+ "_M_" +  M / 1024 / 1024 + "_d_" + d + "_u_" + u + "_m_" + m + "_tp_" + GeneralUtil.throughputSamplingRate;
		PrintWriter pw = new PrintWriter(new File(resultFilePath));
		Double res = 0.0;
		
		double duration = 0;

		for (int i = 0; i < loops; i++) {
			initCM(sketchMin);
			initUnioning(sketchMin);
			//long startTime = System.nanoTime();
			for (int j = 0; j < totalNum; j++) {
				for (int k = 0; k < d; k++) {
                	C[k][(GeneralUtil.intHash(dataFlowID.get(j) ^ S[k]) % w + w) % w].encode(dataElemID.get(j));
                }
			}	
			
			//long endTime = System.nanoTime();
			
			//duration += 1.0 * (endTime - startTime) / 1000000000;
			totalNum = 10000;
			long startTime = System.nanoTime();
		for (int j = 0; j < totalNum; j++) {

			int estimate = Integer.MAX_VALUE;
			int temp = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[0]) % w + w) % w;
			if (C[0][0].getDataStructureName().equals("Bitmap")) {
			for (int k=0;k<D.getUnitSize();k++)	{
				if (C[0][temp].getBitmaps().get(k)) {
					D.getBitmaps().set(k);
				}
				else
					D.getBitmaps().clear(k);
			}
			for(int ii = 1; ii < d; ii++) {
				int jj = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[ii]) % w + w) % w;
				for (int k=0;k<C[0][temp].getUnitSize();k++)	{
					if (!C[ii][jj].getBitmaps().get(k)) {
						D.getBitmaps().clear(k);
					}
				}
			}
			estimate=D.getValue();
			}
			if (C[0][0].getDataStructureName().equals("FMsketch")) {
				for (int k=0;k<mValueFM ;k++)	{
					
						D.getFMSketches()[k]=C[0][temp].getFMSketches()[k];
					
				}
				for(int ii = 1; ii < d; ii++) {
					int jj = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[ii]) % w + w) % w;
					for (int k=0;k<mValueFM ;k++)	{
						D.getFMSketches()[k].and(C[ii][jj].getFMSketches()[k]);
					}
				}
				estimate=D.getValue();
				/*for (int k=0;k<mValueFM ;k++)	{
					D.getFMSketches()[k];
					for(int l=0;l<FMsketchSize;l++)
					if (C[0][temp].getFMSketches()[k].get(l)) {
						D.getFMSketches()[k].set(l);
					}
					else
						D.getFMSketches()[k].clear(l);
				}
				for(int ii = 1; ii < d; ii++) {
					int jj = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[ii]) % w + w) % w;
					for (int k=0;k<mValueFM ;k++)	{
						for(int l=0;l<FMsketchSize;l++)
							if (!C[ii][jj].getFMSketches()[k].get(l)) {	
								D.getFMSketches()[k].clear(l);
						}
					}
				}
				estimate=D.getValue();*/
			}
			if (C[0][0].getDataStructureName().equals("HyperLogLog")) {
				/*for (int k=0;k<mValueHLL ;k++)	{
					//for(int l=0;l<HLLSize;l++)
					D.getHLLs()[k] = C[0][temp].getHLLs()[k];
					//if (C[0][temp].getHLLs()[k].get(l)) {
					//	D.getHLLs()[k].set(l);
					//}
					//else
						//D.getHLLs()[k].clear(l);
				}
				for(int ii = 1; ii < d; ii++) {
					int jj = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[ii]) % w + w) % w;
					for (int k=0;k<mValueHLL;k++)	{
							if (getBitSetValue(C[ii][jj].getHLLs()[k])<getBitSetValue(D.getHLLs()[k])) {	
								D.getHLLs()[k]=C[ii][jj].getHLLs()[k];
								//D.setBitSetValue(k,getBitSetValue(C[i][j].getHLLs()[k]));
								if (getBitSetValue(C[ii][jj].getHLLs()[k])!=getBitSetValue(D.getHLLs()[k])) { 
									System.out.println("error--------------"+getBitSetValue(C[i][j].getHLLs()[k])+"  "+getBitSetValue(D.getHLLs()[k]));
									System.out.println(D.getHLLs()[k]);
									System.out.println(C[ii][jj].getHLLs()[k]);

								}
							}
					}
				}
				estimate=D.getValue();*/
				
				HyperLogLog[] hll = new HyperLogLog[d];
					for(int ii = 0; ii < d; ii++) {
						int jj = (GeneralUtil.intHash(GeneralUtil.FNVHash1(dataFlowID.get(j)) ^ S[ii]) % w + w) % w;
						hll[ii] =(HyperLogLog) C[ii][jj];
						
					}
				int zeros = 0;
				double result = 0.0;
				for (int k=0;k<mValueHLL;k++) {
					int minValue = Integer.MAX_VALUE;
				for(int ii=0;ii<d;ii++) {
					int temp22 = getBitSetValue(hll[ii].getHLLs()[k]);
					if (temp22 <minValue) minValue = temp22;
					
				}
				if (minValue ==0) zeros ++;
				result +=  Math.pow(2, -1.0 * minValue);
				
				}
				estimate =(int) HyperLogLog.getValue(result,zeros);
			}
		}
long endTime = System.nanoTime();
		
		duration += 1.0 * (endTime - startTime) / 1000000000;
		}
		
		res = 1.0 * totalNum / (duration / loops);
		//System.out.println("Average execution time: " + 1.0 * duration / loops + " seconds");
		System.out.println(C[0][0].getDataStructureName() + "\t Average Throughput: " + 1.0 * totalNum / (duration / loops) + " packets/second" );
		pw.println(res.intValue());
		pw.close();
	}
}
