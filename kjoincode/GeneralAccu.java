package kjoincode;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;


/** A general framework for count min. The elementary data structures to be shared here can be counter, bitmap, FM sketch, HLL sketch. Specifically, we can
 * use counter to estimate flow sizes, and use bitmap, FM sketch and HLL sketch to estiamte flow cardinalities. 
 */

public class GeneralAccu {
	public static Random rand = new Random();
	
	public static int n = 0; 						// total number of packets
	public static int flows = 0; 					// total number of flows
	public static int avgAccess = 0; 				// average memory access for each packet
	public static final int M = 1024 * 1024 * 8; 	// total memory space Mbits	
	public static GeneralDataStructure[][] CM;
	public static GeneralDataStructure[][] CBF;
	public static int u = 32;				// size of each unit
	public static final int d = 4; 			// the nubmer of hash functions
	public static int[] S = new int[d];		// random seeds for Count Min and Counting Bloom filter
	
	/** parameters for count-min */
	public static int wCM = 1;				// the number of columns in Count Min

	/** parameters for CBF */
	public static int wCBF = 1;			// number of counter in CBF


	public static void main(String[] args) throws FileNotFoundException {
		init();
		encodeSize(GeneralUtil.dataStreamForFlowSize);
        estimateSize(GeneralUtil.dataSummaryForFlowSize);
	}
	
	// Init the Count Min and CBF
	public static void init() {
		CM = generateCM();
		CBF = generateCBF();
		generateCMRamdonSeeds();
	}

	
	// Generate couter base Couter Min for flow size measurement.
	public static Counter[][] generateCM() {
		wCM = (M / u) / d;
		Counter[][] B = new Counter[d][wCM];
		for (int i = 0; i < d; i++) {
			for (int j = 0; j < wCM; j++) {
				B[i][j] = new Counter(1, u);
			}
		}
		return B;
	}
	
	// Generate Couter Bloom Filter for flow cardinality size measurement.
	public static Counter[][] generateCBF() {
		wCBF = (M / u);
		Counter[][] B = new Counter[1][wCBF];
		for (int j = 0; j < wCBF; j++) {
			B[0][j] = new Counter(1, u);
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

	/** Encode elements to the Count Min for flow size measurment. */
	public static void encodeSize(String filePath) throws FileNotFoundException {
		Scanner sc = new Scanner(new File(filePath));
		n = 0;
		
		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String flowid = GeneralUtil.getSizeFlowID(strs, true);
			n++;
			
			/** encode for CM */
			Double minValCM = (double) Integer.MAX_VALUE;
            for (int i = 0; i < d; i++) {
                int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCM + wCM) % wCM;
                minValCM = Math.min(minValCM, CM[i][j].getValue());
            }
            for (int i = 0; i < d; i++) {
	            int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCM + wCM) % wCM;
	            if (CM[i][j].getValue() == (minValCM)) {
	            	CM[i][j].encode();           
	            }
            }
            
            /** encode for CBF */
            Double minValCBF = (double) Integer.MAX_VALUE;
            for (int i = 0; i < d; i++) {
                int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCBF + wCBF) % wCBF;
                minValCBF = Math.min(minValCBF, CBF[0][j].getValue());
            }
            for (int i = 0; i < d; i++) {
	            int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCBF + wCBF) % wCBF;
	            if (CBF[0][j].getValue() == (minValCBF)) {
	            	CBF[0][j].encode();           
	            }
            }
		}
		System.out.println("Total number of encoded pakcets: " + n);
		sc.close();
	}

	/** Estiamte flow sizes. */
	public static void estimateSize(String filePath) throws FileNotFoundException {
		System.out.println("Estimating Flow SIZEs..." ); 
		Scanner sc = new Scanner(new File(filePath));
		Double cmErr = 0.0;
		Double cbfErr = 0.0;

		while (sc.hasNextLine()) {
			String entry = sc.nextLine();
			String[] strs = entry.split("\\s+");
			String flowid = GeneralUtil.getSizeFlowID(strs, false);
			int num = Integer.parseInt(strs[strs.length-1]);

			Double estimateCM = (double) Integer.MAX_VALUE;
			Double estimateCBF = (double) Integer.MAX_VALUE;
			
			for(int i = 0; i < d; i++) {
				int jCM = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCM + wCM) % wCM;
				estimateCM = Math.min(estimateCM, CM[i][jCM].getValue());
				
				int jCBF = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid) ^ S[i]) % wCBF + wCBF) % wCBF;	
				estimateCBF = Math.min(estimateCBF, CBF[0][jCBF].getValue());
			}
			cmErr += Math.abs(estimateCM - 1.0*num);
			cbfErr += Math.abs(estimateCBF - 1.0*num);
		}
		sc.close();
		System.out.println("CM error: " + cmErr.intValue() + "\t" + cmErr/n);
		System.out.println("CBF error: " + cbfErr.intValue() + "\t" + cbfErr/n);
	}
}
