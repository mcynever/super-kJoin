package skjoincode;


/*public class HyperLogLog extends GeneralDataStructure {
	// parameters for HLL 
	public static String name = "HyperLogLog";				// the data structure name
	public static int HLLSize; 								// unit size for each register
	public static int m;									// the number of registers in the HLL sketch
	public static int maxRegisterValue;
	public static double alpha;
	
	public int[] HLL;
	
	public HyperLogLog(int m, int size) {
		super();
		HLLSize = size;
		HyperLogLog.m = m;
		maxRegisterValue = (int) (Math.pow(2, size) - 1);
		HLL = new int[m];
		alpha = getAlpha(m);
	}

	@Override
	public String getDataStructureName() {
		return name;
	}

	@Override
	public int getUnitSize() {
		return HLLSize*m;
	}

	@Override
	public int getValue() {
		if (m == 1) {
			return HLL[0];
		}
		return getValue(HLL);
	}
	
	@Override
	public void encode() {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int k = r % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[k] = Math.max(HLL[k], leadingZeros);
	}
	
	@Override
	public void encode(int elementID) {
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % m + m) % m;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[k] = Math.max(HLL[k], leadingZeros);
	}

	
	@Override
	public void encode(String flowID, int[] s) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encode(int flowID, int[] s) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encode(String elementID) {
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % m + m) % m;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[k] = Math.max(HLL[k], leadingZeros);
	}
	
	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int ms = s.length;
		int hash_val = GeneralUtil.intHash(elementID);
		int j = (hash_val % ms + ms) % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int ms = s.length;
		int j = (GeneralUtil.intHash(elementID) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		HLL[i] = Math.max(HLL[i], leadingZeros);
	}
	
	
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		int[] sketch = new int[ms];
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sketch[j] = HLL[i];
		}
		return getValue(sketch);
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int[] sketch = new int[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLL[i];
		}
		return getValue(sketch);
	}
	
	public int getValue(int[] sketch) {
		Double result = 0.0;
		int zeros = 0;
		int len = sketch.length;
		for (int i = 0; i < len; i++) {
			if (sketch[i] == 0) zeros++;
			result = result + Math.pow(2, -1.0 * sketch[i]);
		}
		result = alpha * len * len / result;
		if (result <= 5.0 / 2.0 * len) {			// small flows
			result = 1.0 * len * Math.log(1.0 * len / Math.max(zeros, 1));
		} else if (result > Integer.MAX_VALUE / 30.0) {
			result = -1.0 * Integer.MAX_VALUE * Math.log(1 - result / Integer.MAX_VALUE);
		}
		return result.intValue();
		
	}
	
	public double getAlpha(int m) {
		double a;
		if (m == 16) {
			a = 0.673;
		} else if (m == 32) {
			a = 0.697;
		} else if (m == 64) {
			a = 0.709;
		} else {
			a = 0.7213 / (1 + 1.079 / m);
		}
		return a;
	}
}*/


import java.util.BitSet;
import java.util.HashSet;
import java.util.Random;

public class HyperLogLog extends GeneralDataStructure {
	/** parameters for HLL */
	public String name = "HyperLogLog";				// the data structure name
	public int HLLSize; 								// unit size for each register
	public int m;									// the number of registers in the HLL sketch
	public int maxRegisterValue;
	public double alpha;
	//public int[] HLL;
	public BitSet[] HLLMatrix;
	
	public HyperLogLog(int m, int size) {
		super();
		HLLSize = size;
		this.m = m;
		maxRegisterValue = (int) (Math.pow(2, size) - 1);
		HLLMatrix = new BitSet[m];
		for (int i = 0; i < m; i++) {
			HLLMatrix[i] = new BitSet(size);
		}
		alpha = getAlpha(m);
	}

	@Override
	public String getDataStructureName() {
		return name;
	}
	
	@Override
	public BitSet getBitmaps() {
		return null;
	}
	
	@Override
	public int[] getCounters() {
		return null;
	};
	
	@Override
	public BitSet[] getFMSketches() {
		return null;
	};
	
	@Override
	public BitSet[] getHLLs() {
		return HLLMatrix;
	};

	@Override
	public int getUnitSize() {
		return HLLSize*m;
	}

	@Override
	public int getValue() {
		return getValue(HLLMatrix);
	}
	@Override
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new HyperLogLog[m];
		for(int i=0;i<m;i++) {
			B[i]=new HyperLogLog(w,5);
			for(int j=0;j<w/m;j++)
				B[i].getHLLs()[j]=this.HLLMatrix[i*(w/m)+j];
		}
		return B;
	}
	@Override
	public void encode() {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int k = r % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	
	@Override
	public void encode(String elementID) {
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % m + m) % m;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	
	@Override
	public void encode(int elementID) {
 		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1; // % hyperLogLog_size + hyperLogLog_size) % hyperLogLog_size;
		int k = (hash_val % m + m) % m;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[k]) < leadingZeros) {
			setBitSetValue(k, leadingZeros);
		}
	}
	
	@Override
	public void encode(String flowID, int[] s) {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int ms = s.length;
		int j = r % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encode(int flowID, int[] s) {
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % s.length;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encode(long flowID, long elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encode(int flowID, int elementID, int[] s) {
		int ms = s.length;
		int hash_val = GeneralUtil.intHash(elementID);
		int j = (hash_val % ms + ms) % ms;
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sketch[j] = HLLMatrix[i];
		}
		return getValue(sketch);
	}
	
	public int getValue(long flowID, int[] s) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sketch[j] = HLLMatrix[i];
		}
		return getValue(sketch);
	}
	
	public int getValue(BitSet[] sketch) {
		Double result = 0.0;
		int zeros = 0;
		int len = sketch.length;
		for (int i = 0; i < len; i++) {
			if (getBitSetValue(sketch[i]) == 0) zeros++;
			result = result + Math.pow(2, -1.0 * getBitSetValue(sketch[i]));
		}
		result = alpha * len * len / result;
		if (result <= 5.0 / 2.0 * len) {			// small flows
			result = 1.0 * len * Math.log(1.0 * len / Math.max(zeros, 1));
		} else if (result > Integer.MAX_VALUE / 30.0) {
			result = -1.0 * Integer.MAX_VALUE * Math.log(1 - result / Integer.MAX_VALUE);
		}
		return result.intValue();
	}
	
	public static int getBitSetValue(BitSet b) {
		int res = 0;
		for (int i = 0; i < b.length(); i++) {
			if (b.get(i)) res += Math.pow(2, i);
		}
		return res;
	}
	
	public void setBitSetValue(int index, int value) {
		int i = 0;
		while (value != 0 && i < HLLSize) {
			if ((value & 1) != 0) {
				HLLMatrix[index].set(i);
			} else {
				HLLMatrix[index].clear(i);
			}
			value = value >>> 1;
			i++;
		}
	}
	
	public double getAlpha(int m) {
		double a;
		if (m == 16) {
			a = 0.673;
		} else if (m == 32) {
			a = 0.697;
		} else if (m == 64) {
			a = 0.709;
		} else {
			a = 0.7213 / (1 + 1.079 / m);
		}
		return a;
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int ms = s.length;
		//int j = ((GeneralUtil.FNVHash1(flowID^GeneralvSkt.c1)^GeneralUtil.FNVHash1(elementID^GeneralvSkt.c2)) % ms + ms) % ms;
		//int j = ((GeneralUtil.FNVHash1(flowID,elementID)) % ms + ms) % ms;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		//int j = ((GeneralUtil.FNVHash1(elementID)) % ms + ms) % ms;
		//System.out.println(elementID^flowID);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
		
	}
	@Override
	public void encodeCSegment(long flowID, long elementID, int[] s, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int pp=(GeneralUtil.FNVHash1(flowID^(long)s[j])%2+2)%2;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j*2 * w + k*2+pp;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	

	
	@Override
	public void encodevCMSegment(long flowID, long elementID, int[] s, int ss, int w,int segmentLength) {
		int ms = s.length;
		w/=segmentLength;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID ^ s[j/segmentLength]) ^ ss) % w + w) % w;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		i = i*segmentLength +j%segmentLength;
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encodevCSSegment(long flowID, long elementID, int[] s, int ss, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int pp=(GeneralUtil.FNVHash1(flowID^(((long)s[j])<<32)^((long)ss))%2+2)%2;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
		i=i*2+pp;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(elementID)^GeneralUtil.FNVHash1(flowID)) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}

	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int ms = s.length;
		int j = (GeneralUtil.intHash(elementID) % ms + ms) % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(elementID);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}

	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public void encodevCMSegment(long flowID, int[] s, int ss, int w, int r) {
		int ms = s.length;
		//int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
		int hash_val = GeneralUtil.FNVHash1(String.valueOf(r));
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	

	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int r = rand.nextInt(Integer.MAX_VALUE);
		int j = r % ms;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		int hash_val = GeneralUtil.intHash(r);
		int leadingZeros = Integer.numberOfLeadingZeros(hash_val) + 1;
		leadingZeros = Math.min(leadingZeros, maxRegisterValue);
		if (getBitSetValue(HLLMatrix[i]) < leadingZeros) {
			setBitSetValue(i, leadingZeros);
		}
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
		}
		int result = getValue(sketch);
		return result;
	}
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
		}
		int result = getValue(sketch);
		return result;
	}
	@Override
	public int getvCMValueSegment(long flowID, int[] s, int ss, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
			sketch[j] = HLLMatrix[i];
		}
		int result = getValue(sketch);
		return result;
	}
	@Override
	public int getvCSValueSegment(long flowID, int[] s, int ss, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		BitSet[] sketch1 = new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int pp=(GeneralUtil.FNVHash1(flowID^(((long)s[j])<<32)^((long)ss))%2+2)%2;
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
			sketch[j] = HLLMatrix[i*2+pp];
			sketch1[j] = HLLMatrix[i*2+(1-pp)];
		}
		int result = getValue(sketch)-getValue(sketch1);
		return result;
	}
	
	@Override
	public int getValueSegment1(long flowID, int[] s, int w) {
		int ms = s.length;
		BitSet[] sketch = new BitSet[ms];
		BitSet[] sketch1 =new BitSet[ms];
		for (int j = 0; j < ms; j++) {
			int pp=(GeneralUtil.FNVHash1(flowID^(long)s[j])%2+2)%2;
			int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			int i = j * 2 * w + k*2+pp;
			int ii= j*2*w+k*2+(1-pp);
			//int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sketch[j] = HLLMatrix[i];
			sketch1[j] = HLLMatrix[ii];
		}
		int result = getValue(sketch)-getValue(sketch1);
		return result;
	}
	/**
	 * reduce the number of estimator used to reduce bias for small flows.
	 */
	@Override
	public int getOptValueSegment(String flowID, int[] s, int w, int sample_ratio) {
		int ms = s.length;
		int[] indexes = new int[ms];
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			indexes[j] = i;
		}
		int mss = ms / sample_ratio, i = 0;
		BitSet[] sketch_sampled = new BitSet[mss];
		HashSet<Integer> index_sampled = new HashSet<>();
		while (index_sampled.size() < mss) {
			if (rand.nextDouble() < 1.0 / sample_ratio) {
				index_sampled.add(indexes[i]);
				//System.out.println("Sampled!");
			}
			i = (i + 1) % ms;
		}
		int j = 0;
		for (int k: index_sampled) {
			sketch_sampled[j++] = HLLMatrix[k];
		}
		//System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return getValue(sketch_sampled);
	}
	
	@Override
	public GeneralDataStructure getvCMminValueSegment(GeneralDataStructure[] gds, long flowid, int[] S, int[] SS, int w, int segmentLength) {
		HyperLogLog[] hll=(HyperLogLog[]) gds;
		w /= segmentLength;
		for(int i=0; i<HLLMatrix.length/segmentLength;i++) {
			int j = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid^ S[i])  ^ SS[0]) % w + w) % w;
			for (int pp = 0; pp<segmentLength; pp++) {

				this.HLLMatrix[i*segmentLength+pp]=hll[0].HLLMatrix[j*segmentLength+pp];
			}
		}
		for (int i = 1; i < gds.length; i++) {
			for(int j =  0; j<HLLMatrix.length/segmentLength; j++) {
				int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowid^ S[j])  ^ SS[i]) % w + w) % w;
				for (int pp = 0; pp<segmentLength; pp++) {

					if (getBitSetValue(hll[i].HLLMatrix[k*segmentLength+pp]) < getBitSetValue(this.HLLMatrix[j*segmentLength+pp]))
						this.HLLMatrix[j*segmentLength+pp] = hll[i].HLLMatrix[k*segmentLength+pp];
				}
			}
		}
		return this;
	}
	
	@Override
	public GeneralDataStructure getspreadnoise(GeneralDataStructure[] gds) {
		HyperLogLog[] hll=(HyperLogLog[]) gds;
		Random ran=new Random();
		int w = this.HLLMatrix.length;
		for(int i=0; i<HLLMatrix.length;i++) {
			int r=ran.nextInt();
			int j = ((r^GeneralvCountMin.S[i]^GeneralvCountMin.SS[0]) % w + w) % w;
			this.HLLMatrix[i]=hll[0].HLLMatrix[j];
		}
		for (int i = 1; i < gds.length; i++) {
			for(int j =  0; j<HLLMatrix.length; j++) {
				int r=ran.nextInt();
				int k = ((r^GeneralvCountMin.S[j]^GeneralvCountMin.SS[i]) % w + w) % w;
				if (getBitSetValue(hll[i].HLLMatrix[k]) < getBitSetValue(this.HLLMatrix[j]))
				this.HLLMatrix[j] = hll[i].HLLMatrix[k];
			}
		}
		return this;
	}
	
	
	
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		HyperLogLog hll = (HyperLogLog) gds;
		for (int i = 0; i < m; i++) {
			if (getBitSetValue(HLLMatrix[i]) < getBitSetValue(hll.HLLMatrix[i]))
				HLLMatrix[i] = hll.HLLMatrix[i];
		}
		return this;
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		HyperLogLog hll = (HyperLogLog) gds;		
		for(int j=0;j<w;j++) {
			if (getBitSetValue(HLLMatrix[i]) < getBitSetValue(hll.HLLMatrix[i*w+j]))
				HLLMatrix[i] = hll.HLLMatrix[i*w+j];
		}
		return this;
	}
}
