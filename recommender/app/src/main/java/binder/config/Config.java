/*
	Copyright 2020 Florestan De Moor & Guillaume Longrais
	
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

import org.apache.mahout.cf.taste.eval.FoldDataSplitter;
import org.slf4j.Logger;

import java.util.List;

public class Config implements AbstractConfig {

	private String data; /* Set if the data will be local or online*/
	private String dataset; /* Path to the dataset file */
	private String resultPath;
	private String keyPath;
	private String testPath;
	private int nbUserPerFile;
	private int nbRecommendation;
	private List<String> configs;
	private boolean normalize; /* Normalize dataset by subtracting user mean rating */
	private boolean binarize; /* Binarize ratings into 0 or 1 */
	private int folds; 
	private boolean doIREval; /* Run IRStats evaluation */
	private List<String> strategies; /* Strategy for candidate item selection */
	private int r; /*
	 * Max number of recommendations for precision@r and recall@r, will go from 5 to
	 * r with step 5
	 */
	private boolean oneR; /* Run eval with only one r instead of iterations */
	private float threshold; /* Like-threshold for ratings */
	private int nbruns; /* Number of runs for each evaluation */
	private boolean doTimeEval; /* Run the time performance evaluation */
	private boolean doPerUserEval; /* Run pred and irstats per user evaluation */
	private boolean doPredEval; /* Run RMSE and MAE evaluation */

	public void setData(String s) {
		this.data = s;
	}

	public String getData() {
		return this.data;
	}

	public void setResultPath(String s) {
		this.resultPath= s;
	}

	public String getResultPath() {
		return this.resultPath;
	}

	public void setKeyPath(String s) {
		this.keyPath = s;
	}

	public String getKeyPath() {
		return this.keyPath;
	}

	public void setTestPath(String s) {
		this.testPath = s;
	}

	public String getTestPath() {
		return this.testPath;
	}

	public void setNbUserPerFile(int n) {
		this.nbUserPerFile = n;
	}

	public int getNbUserPerFile() {
		return this.nbUserPerFile;
	}

	public void setNbRecommendation(int n) {
		this.nbRecommendation = n;
	}

	public int getNbRecommendation() {
		return this.nbRecommendation;
	}

	public void setDataset(String s) {
		this.dataset = s;
	}

	public String getDataset() {
		return this.dataset;
	}

	public void setConfigs(List<String> l) {
		this.configs = l;
	}

	public List<String> getConfigs() {
		return this.configs;
	}

	public void setNormalize(boolean b) {
		this.normalize = b;
	}

	public boolean getNormalize() {
		return this.normalize;
	}

	public void setBinarize(boolean b) {
		this.binarize = b;
	}

	public boolean getBinarize() {
		return this.binarize;
	}
	
	public void setFolds(int n){
		this.folds=n;
	}
	
	public int getFolds() {
		
		return this.folds;
	}

	public boolean getFilterBBCF() {
		
		return false;
	}
	
	public void setStrategies(List<String> l) {
		this.strategies = l;
	}

	public List<String> getStrategies() {
		
		return this.strategies;
	}

	
	
	public void setDoIREval(boolean b) {
		this.doIREval = b;
	}


	public boolean getDoIREval() {
		return this.doIREval;
	}

	public float getThreshold() {
		// TODO Auto-generated method stub
		return this.threshold;
	}
	
	public void setThreshold(float t) {
		
		this.threshold=t;
	}

	

	public boolean getDoPerUserEval() {
		return false;
	}
	
	public void setNruns(int n) {
		this.nbruns=n;
	}

	public int getNruns() {
		return this.nbruns;
	}
	
	public void setR(int r) {
		this.r=r;
	}

	public int getR() {
		return this.r;
	}

	public boolean getOneR() {
		return this.oneR;
	}
	
	public void setDoTimeEval(boolean b) {
		this.doTimeEval = b;
	}
	
	public boolean getDoTimeEvel() {
		return this.doTimeEval;
	}
	
	public void setDoPredEval(boolean b) {
		this.doPredEval = b;
	}

	public boolean getDoPredEval() {
		return this.doPredEval;
	}
	
	

	@Override
	public void logConfig(Logger logger) {

		logger.info("Data: {}", this.data);
		logger.info("Dataset path: {}", this.dataset);
		logger.info("Result path: {}", this.resultPath);
		logger.info("Key path: {}", this.keyPath);
		logger.info("Tests path: {}", this.testPath);
		logger.info("Number of recommendations: {}", this.nbRecommendation);
		logger.info("Number of users per file: {}", this.nbUserPerFile);
		logger.info("List of the configuration files of recommender algorithms to run: {}", this.configs.toString());
		logger.info("Normalize: {}", this.normalize);
		logger.info("Binarize: {}", this.binarize);
	}

	

}
