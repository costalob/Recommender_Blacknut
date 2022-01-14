/*
	Copyright 2019 Florestan De Moor

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package binder.config;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NBCFConfig implements AbstractConfig {
	
	private int k; /* Number of nearest biclusters */
	private String biclustering; /* Biclustering algorithm */
	private int minUserSize; /* Minimum number of users in a bicluster (Bimax) */
	private int minItemSize; /* Minimum number of items in a bicluster (Bimax) */
	private float consistency; /* Consistency level (QUBIC) */
	private int size; /* Number of biclusters output (QUBIC) */
	private float overlap; /* Overlap percentage allowed (QUBIC) */
	private boolean bin; /* Binarize ratings (QUBIC) */
	private int ns; /* Number of seeds (xMotif) */
	private int nd; /* Number of samples (xMotif) */
	private int sd; /* Sample size (xMotif) */
	
	public void setK(int kk) {
		this.k = kk;
	}
	
	public int getK() {
		return this.k;
	}
	
	public void setBiclustering(String s) {
		this.biclustering = s;
	}
	
	public String getBiclustering() {
		return this.biclustering;
	}
	
	public void setMinUserSize(int n) {
		this.minUserSize = n;
	}
	
	public int getMinUserSize() {
		return this.minUserSize;
	}
	
	public void setMinItemSize(int n) {
		this.minItemSize = n;
	}
	
	public int getMinItemSize() {
		return this.minItemSize;
	}
	
	public void setConsistency(float c) {
		this.consistency = c;
	}
	
	public float getConsistency() {
		return this.consistency;
	}
	
	public void setSize(int n) {
		this.size = n;
	}
	
	public int getSize() {
		return this.size;
	}
	
	public void setOverlap(float f) {
		this.overlap = f;
	}
	
	public float getOverlap() {
		return this.overlap;
	}
	
	public void setNs(int n) {
		this.ns = n;
	}
	
	public int getNs() {
		return this.ns;
	}
	
	public void setNd(int n) {
		this.nd = n;
	}
	
	public int getNd() {
		return this.nd;
	}
	
	public void setSd(int n) {
		this.sd = n;
	}
	
	public int getSd() {
		return this.sd;
	}
	
	public void setBin(boolean b) {
		this.bin = b;
	}
	
	public boolean getBin() {
		return this.bin;
	}
	
	@Override
	public void logConfig(Logger logger) {
		logger.info("NBCF");
		logger.info("Number of nearest biclusters: {}", String.valueOf(this.k));
		logger.info("Biclustering algorithm: {}", this.biclustering);
		logger.info("Minimum user size for biclusters: {} (Bimax)", this.minUserSize);
		logger.info("Minimum item size for biclusters: {} (Bimax)", this.minItemSize);
		logger.info("Consistency level: {} (QUBIC)", this.consistency);
		logger.info("Number of biclusters output: {} (QUBIC)", this.size);
		logger.info("Overlap percentage allowed: {} (QUBIC)", this.overlap);
		logger.info("Binarize ratings: {} (QUBIC)", this.bin);
		logger.info("Number of seeds: {} (xMotif)", this.ns);
		logger.info("Number of samples: {} (xMotif)", this.nd);
		logger.info("Sample size: {} (xMotif)", this.sd);
	}

}
