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

public class COCLUSTConfig implements AbstractConfig {
	
	private int nbUserClusters; /* Number of user clusters */
	private int nbItemClusters; /* Number of item clusters */
	private int nbMaxIterations; /* Maximum number of iterations */
	
	public void setNbUserClusters(int n) {
		this.nbUserClusters = n;
	}
	
	public int getNbUserClusters() {
		return this.nbUserClusters;
	}
	
	public void setNbItemClusters(int n) {
		this.nbItemClusters = n;
	}
	
	public int getNbItemClusters() {
		return this.nbItemClusters;
	}
	
	public void setNbMaxIterations(int n) {
		this.nbMaxIterations = n;
	}
	
	public int getNbMaxIterations() {
		return this.nbMaxIterations;
	}
	
	@Override
	public void logConfig(Logger logger) {
		logger.info("COCLUST");
		logger.info("Number of user clusters: {}", this.nbUserClusters);
		logger.info("Number of item clusters: {}", this.nbItemClusters);
		logger.info("Maximum number of iterations: {}", this.nbMaxIterations);
	}

}
