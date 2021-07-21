package kjoincode;
import java.util.BitSet;
import java.util.HashSet;

/** The implentation of conter */
public class Counter extends GeneralDataStructure {
	/** parameters for counter */
	public String name = "Counter";							// counter data structure name
	public int counterSize;									// unit counter size
	public int m;											// the number of counters in the counter array
	public int counterThreshold;								// maximum value of the counter data structure
	
	public int[] counters;									
	
	public Counter(int m, int size) {
		super();
		counterSize = size;
		this.m = m;
		counters = new int[m];
		counterThreshold = (int) (Math.pow(2, size) - 1);
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
		return counters;
	};
	
	@Override
	public BitSet[] getFMSketches() {
		return null;
	};
	
	@Override
	public BitSet[] getHLLs() {
		return null;
	};	
	
	@Override
	public int getUnitSize() {
		return counterSize*m;
	}

	@Override
	public int getValue() {
		int sum = 0;
		for (int i : counters)
		    sum += i;
		return sum;
	}
	
	@Override
	public void encode() {
		int k = rand.nextInt(m);
		// Check the threshold first, then encode an element in to the counter.
		if (counters[k] < counterThreshold) {
			counters[k]++;
		}
	}
	@Override
	public void encodecs(int sign) {
		int k = rand.nextInt(m);
		if(sign==0)
			counters[k]++;
		else
			counters[k]--;
		// Check the threshold first, then encode an element in to the counter.
		//if (counters[k] < counterThreshold) {
			//counters[k]++;
		//}
	}
	
	@Override
	public void encode(String elementID) {
		int k = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		// Check the threshold first, then encode an element in to the counter.
		if (counters[k] < counterThreshold) {
			counters[k]++;
		}
	}
	
	@Override
	public void encode(int elementID) {
		int k = 0;
		if (m != 1) k = (GeneralUtil.intHash(elementID) % m + m) % m;
		// Check the threshold first, then encode an element in to the counter.
		if (counters[k] < counterThreshold) {
			counters[k]++;
		}
	}
	@Override
	public GeneralDataStructure[] getsplit(int m,int w) {
		GeneralDataStructure[]  B=new Counter[m];
		for(int i=0;i<m;i++) {
			B[i]=new Counter(w,w/m);
			System.arraycopy(this.getCounters(),i*(w/m),B[i].getCounters(),0,w/m);
		}
		return B;
	}
	@Override
	public void encode(String flowID, int[] s) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	@Override
	public void encodeSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	@Override
	public void encodeCSegment(long flowID, long elementID, int[] s, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int pp=(GeneralUtil.FNVHash1(flowID^(long)s[j])%2+2)%2;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j*2 * w + k*2+pp;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	
	@Override
	public void encodeSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	@Override
	public void encodedvSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int j = Integer.numberOfLeadingZeros(rand.nextInt());
		if(j >= ms)
			j = ms - 1;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		//if (counters[i] < counterThreshold) {
			counters[i] ++;
		//}
	}
	
	@Override
	public void encodevCMSegment(long flowID, int[] s, int ss, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	@Override
	public void encodeSegment(int flowID, int[] s, int w) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
		
	@Override
	public void encode(int flowID, int[] s) {
		int ms = s.length;
		int j = rand.nextInt(ms);
		int i = (GeneralUtil.intHash(flowID ^ s[j]) % m + m) % m;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}

	@Override
	public void encode(String flowID, String elementID, int[] s) {
		int ms = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	@Override
	public void encodeSegment(long flowID, long elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID^flowID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	@Override
	public void encodevCMSegment(long flowID, long elementID, int[] s, int ss, int w) {
		int ms = s.length;
		int j = ((GeneralUtil.FNVHash1(flowID^elementID)) % ms + ms) % ms;
		int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	@Override
	public void encodeSegment(String flowID, String elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.FNVHash1(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	@Override
	public void encodeSegment(int flowID, int elementID, int[] s, int w) {
		int m = s.length;
		int j = (GeneralUtil.intHash(elementID) % m + m) % m;
		int k = (GeneralUtil.intHash(flowID ^ s[j]) % w + w) % w;
		int i = j * w + k;
		if (counters[i] < counterThreshold) {
			counters[i] ++;
		}
	}
	
	@Override
	public int getValue(String flowID, int[] s) {
		int ms = s.length;
		int sum = 0;
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % m + m) % m;
			sum += counters[i];
		}
		return sum;
	}
	
	@Override
	public int getValueSegment(String flowID, int[] s, int w) {
		int ms = s.length;
		int sum = 0;
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sum += counters[i];
		}
		return sum;
	}
	
	@Override
	public int getValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int sum = 0;
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sum += counters[i];
		}
		return sum;
	}
	
	@Override
	public int getvCMValueSegment(long flowID, int[] s, int ss, int w) {
		int ms = s.length;
		int sum = 0;
		for (int j = 0; j < ms; j++) {
			int i = (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j] ^ ss) % w + w) % w;
			sum += counters[i];
		}
		return sum;
	}
	@Override
	public int getdvValueSegment(long flowID, int[] s, int w) {
		int ms = s.length;
		int sum = 0;
		for (int j = 0; j < ms; j++) {
			int i = j * w + (GeneralUtil.intHash(GeneralUtil.FNVHash1(flowID) ^ s[j]) % w + w) % w;
			sum += counters[i];
		}
		return sum;
	}
	@Override
	public int getdvValue() {
		double sum = 0.0;
		double rate = 1.0;
		for (int i=0;i<counters.length;i++) {
			sum += (double)counters[i] *rate;
		}
		return (int)sum;
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
		int sum = 0;
		for (int k: index_sampled) {
			sum += counters[k];
		}
		//System.out.println("Sampled virtual sketch size: " + index_sampled.size() + "\nOriginal virtual sketch size: " + ms);
		return sum;
	}

	@Override
	public void encode(int flowID, int elementID, int[] s) {
		// TODO Auto-generated method stub
		
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds,int w,int i) {
		Counter c=(Counter)gds;
		for(int j=0;j<w;j++)
			counters[i]+=c.counters[i*w+j];
		return this;
	}
	@Override
	public GeneralDataStructure join(GeneralDataStructure gds) {
		Counter c = (Counter) gds;
		for (int i = 0; i < m; i++)
		    counters[i] += c.counters[i];
		return this;
	}

	@Override
	protected void setBitSetValue(int k, int bitSetValue) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected double returnAlpha() {
		// TODO Auto-generated method stub
		return 0;
	}
}
