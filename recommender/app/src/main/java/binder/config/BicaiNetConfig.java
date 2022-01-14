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

import org.apache.logging.log4j.Logger;

public class BicaiNetConfig implements AbstractConfig {
	
	private float wr; /* Row weight */
	private float wc; /* Row column */
	private float lambda; /* Residue threshold */
	private int maxNbIt; /* Number of iterations */
	private int supIt; /* Number of iterations between suppressions */
	private int o; /* Number of biclusters to output */
	
	public void setWr(float x) {
		this.wr = x;
	}
	
	public float getWr() {
		return this.wr;
	}
	
	public void setWc(float x) {
		this.wc = x;
	}
	
	public float getWc() {
		return this.wc;
	}
	
	public void setLambda(float x) {
		this.lambda = x;
	}
	
	public float getLambda() {
		return this.lambda;
	}
	
	public void setMaxNbIt(int n) {
		this.maxNbIt = n;
	}
	
	public int getMaxNbIt() {
		return this.maxNbIt;
	}
	
	public void setSupIt(int n) {
		this.supIt = n;
	}
	
	public int getSupIt() {
		return this.supIt;
	}
	
	public void setO(int n) {
		this.o = n;
	}
	
	public int getO() {
		return this.o;
	}
	
	@Override
	public void logConfig(Logger logger) {
		logger.info("Bic-aiNet");
		logger.info("Row weight: {}", this.wr);
		logger.info("Column weight: {}", this.wc);
		logger.info("Residue threshold: {}", this.lambda);
		logger.info("Number of iterations: {}", this.maxNbIt);
		logger.info("Number of iterationw between suppressions: {}", this.supIt);
		logger.info("Number of biclusters to output: {}", this.o);
	}

}
