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

public class UBKNNConfig implements AbstractConfig {
	
	private int k; /* Number of nearest neighbors as in kNN */
	private String similarity; /* Similarity metric */
	
	public void setK(int kk) {
		this.k = kk;
	}
	
	public int getK() {
		return this.k;
	}
	
	public void setSimilarity(String s) {
		this.similarity = s;
	}
	
	public String getSimilarity() {
		return this.similarity;
	}

	@Override
	public void logConfig(Logger logger) {
		logger.info("UBKNN");
		logger.info("Number of nearest neighbors: {}", String.valueOf(this.k));
		logger.info("Similarity metric: {}", this.similarity);
	}

	

}
