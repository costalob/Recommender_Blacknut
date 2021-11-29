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
package binder;

import binder.config.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserBiclusterSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.UncenteredCosineSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.SpearmanCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.JaccardUserBiclusterSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserBiclusterNeighborhood;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.CandidateItemsStrategy;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.impl.common.AbstractBiclusteringAlgorithm;
import org.apache.mahout.cf.taste.impl.common.BicaiNet;
import org.apache.mahout.cf.taste.impl.common.Biclustering;
import org.apache.mahout.cf.taste.impl.common.Bimax;
import org.apache.mahout.cf.taste.impl.common.COCLUSTBiclustering;
import org.apache.mahout.cf.taste.impl.common.Interval;
import org.apache.mahout.cf.taste.impl.common.QUBIC;
import org.apache.mahout.cf.taste.impl.common.RandomBiclustering;
import org.apache.mahout.cf.taste.impl.common.XMotif;
import org.apache.mahout.cf.taste.impl.eval.Fold;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNBiclusterNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemUserAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.MultiCOCLUSTRecommender;
import org.apache.mahout.cf.taste.impl.recommender.NBCFRecommender;
import org.apache.mahout.cf.taste.impl.recommender.PreferredItemsNeighborhoodCandidateItemsStrategy;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.impl.recommender.RelPlusRandomCandidateItemStrategy;
import org.apache.mahout.cf.taste.impl.recommender.TestItemsCandidateItemStrategy;
import org.apache.mahout.cf.taste.impl.recommender.TestRatingsCandidateItemStrategy;
import org.apache.mahout.cf.taste.impl.recommender.TrainingItemsCandidateItemStrategy;
import org.apache.mahout.cf.taste.impl.recommender.svd.ALSWRFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.Factorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.RatingSGDFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDPlusPlusFactorizer;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.impl.recommender.AdaptativeCOCLUSTRecommender;
import org.apache.mahout.cf.taste.impl.recommender.AllUnknownItemsCandidateItemsStrategy;
import org.apache.mahout.cf.taste.impl.recommender.BBCFRecommender;
import org.apache.mahout.cf.taste.impl.recommender.BCNRecommender;
import org.apache.mahout.cf.taste.impl.recommender.BicaiNetRecommender;
import org.apache.mahout.cf.taste.impl.recommender.COCLUSTRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;

public class BinderRecommenderBuilder implements RecommenderBuilder {
	
	private AbstractConfig config;
	private static Logger logger = LoggerFactory.getLogger(BinderRecommenderBuilder.class);
	private final String strategy;
	private final float threshold;
	
	BinderRecommenderBuilder(AbstractConfig cfg, String strategy, float threshold) {
		this.config = cfg;
		this.strategy = strategy;
		this.threshold = threshold;
	}

	BinderRecommenderBuilder(AbstractConfig cfg, float threshold) {
		this.config = cfg;
		this.strategy = "trainingitems";
		this.threshold = threshold;
	}

	BinderRecommenderBuilder(AbstractConfig cfg) {
		this.config = cfg;
		this.strategy = "trainingitems";
		this.threshold = 0.0f;
	}
	
	public BinderRecommenderBuilder(BBCFConfig cc, String strategy2, char[] threshold2) {
		this.strategy = "";
		this.threshold = 0;
		// TODO Auto-generated constructor stub
	}

	public BinderRecommenderBuilder(AbstractConfig c, String strategy2, char[] threshold2) {
		this.strategy = "";
		this.threshold = 0;
		// TODO Auto-generated constructor stub
	}

	public Recommender buildRecommender(DataModel dataModel, Fold fold) throws TasteException {
		CandidateItemsStrategy s = null;
		if (this.strategy.equals("testratings")) {
			s = new TestRatingsCandidateItemStrategy(fold.getTesting());
		} else if (this.strategy.equals("testitems")) {
			s = new TestItemsCandidateItemStrategy(fold.getTesting());
		} else if (this.strategy.equals("trainingitems")) {
			s = new TrainingItemsCandidateItemStrategy(fold.getTraining());
		} else if (this.strategy.equals("allitems")) {
			s = new TrainingItemsCandidateItemStrategy(dataModel);
		} else if (this.strategy.equals("relplusrandom")) {
			s = new RelPlusRandomCandidateItemStrategy(fold.getTesting(), dataModel, this.threshold);
		} else {
			logger.error("Invalid candidate item selection strategy {}", this.strategy);
			return null;
		}
		return buildRecommender(dataModel, s);
	}
	
	@Override
	public Recommender buildRecommender(DataModel dataModel) throws TasteException {
		return buildRecommender(dataModel, new AllUnknownItemsCandidateItemsStrategy());
	}
	
	public Recommender buildRecommender(DataModel dataModel, CandidateItemsStrategy s) throws TasteException {
		
		/* Recommender algorithm */
		
		if (this.config instanceof UBKNNConfig) { /* User-Based K-Nearest-Neighbors */
			UBKNNConfig cfg = (UBKNNConfig) this.config;
			
			/* Similarity choice */
			UserSimilarity similarity = null;
			if (cfg.getSimilarity().contains("cosine")) {
				similarity = new UncenteredCosineSimilarity(dataModel);
			} else if (cfg.getSimilarity().contains("pearson")) {
				similarity = new PearsonCorrelationSimilarity(dataModel);
			} else if (cfg.getSimilarity().contains("spearman")) {
				similarity = new SpearmanCorrelationSimilarity(dataModel);
			} else {
				logger.error("Invalid similarity parameter {}", cfg.getSimilarity());
				return null;
			}
			
			UserNeighborhood neighborhood = new NearestNUserNeighborhood(cfg.getK(), similarity, dataModel);
			return new GenericUserBasedRecommender(dataModel, neighborhood, similarity, s);
			
		} else if (this.config instanceof IBKNNConfig) { /* Item-Based K-Nearest-Neighbors */
			IBKNNConfig cfg = (IBKNNConfig) this.config;
			
			/* Similarity choice */
			ItemSimilarity similarity = null;
			if (cfg.getSimilarity().contains("cosine")) {
				similarity = new UncenteredCosineSimilarity(dataModel);
			} else if (cfg.getSimilarity().contains("pearson")) {
				similarity = new PearsonCorrelationSimilarity(dataModel);
			} else if (cfg.getSimilarity().contains("log")) {
				similarity = new LogLikelihoodSimilarity(dataModel);
			} else {
				logger.error("Invalid similarity parameter {}", cfg.getSimilarity());
				return null;
			}
			
			return new GenericItemBasedRecommender(dataModel, similarity, s, new PreferredItemsNeighborhoodCandidateItemsStrategy());
			
		} else if (this.config instanceof RandomConfig) { /* Random */
			return new RandomRecommender(dataModel, s);
			
		} else if (this.config instanceof BCNConfig) { /* BCN */
			BCNConfig cfg = (BCNConfig) this.config;
			return new BCNRecommender(dataModel, this.threshold, cfg.getLevel(), s);
			
		} else if (this.config instanceof ItemAvgConfig) { /* Item average */
			return new ItemAverageRecommender(dataModel, s);
			
		} else if (this.config instanceof ItemUserAvgConfig) { /* Item user average */
			return new ItemUserAverageRecommender(dataModel, s);
			
		} else if (this.config instanceof MFConfig) { /* Matrix Factorization */
			MFConfig cfg = (MFConfig) this.config;
			
			
			/* Factorizer algorithm choice */
			Factorizer fact = null;
			if (cfg.getFactorizer().contains("alswr")) {
				fact = new ALSWRFactorizer(dataModel, cfg.getNbfeatures(), cfg.getLambda(), cfg.getNbiterations());
			} else if (cfg.getFactorizer().contains("svd++")) {
				fact = new SVDPlusPlusFactorizer(dataModel, cfg.getNbfeatures(), cfg.getNbiterations());
			} else if (cfg.getFactorizer().contains("sgd")) {
				fact = new RatingSGDFactorizer(dataModel, cfg.getNbfeatures(), cfg.getNbiterations());
			} else {
				logger.error("Invalid factorizer parameter {}", cfg.getFactorizer());
				return null;
			}
			
			return new SVDRecommender(dataModel, fact, s);
			
		} else if (this.config instanceof COCLUSTConfig) { /* COCLUST */
			COCLUSTConfig cfg = (COCLUSTConfig) this.config;
			
			if (cfg.getNbUserClusters() <= 0 || cfg.getNbItemClusters() <= 0 ) {
				return new MultiCOCLUSTRecommender(dataModel, cfg.getNbMaxIterations(), s);
			} else {
				return new COCLUSTRecommender(dataModel, cfg.getNbUserClusters(), cfg.getNbItemClusters(), cfg.getNbMaxIterations(), s);
			}
			
		} else if (this.config instanceof COCLUSTRConfig) { /* COCLUST */
			COCLUSTRConfig cfg = (COCLUSTRConfig) this.config;
			
			if (cfg.getNbUserClusters() <= 0 || cfg.getNbItemClusters() <= 0 ) {
				return new AdaptativeCOCLUSTRecommender(dataModel, cfg.getNbMaxIterations(), s);
			} else {
				return new COCLUSTRecommender(dataModel, cfg.getNbUserClusters(), cfg.getNbItemClusters(), cfg.getNbMaxIterations(), s, cfg.getLambda());
			}
			
		} else if (this.config instanceof NBCFConfig) { /* NBCF */
			NBCFConfig cfg = (NBCFConfig) this.config;
			
			/* Biclustering algorithm choice */
			AbstractBiclusteringAlgorithm algo = null;
			if (cfg.getBiclustering().contains("bimax")) {
				algo = new Bimax(dataModel, cfg.getMinUserSize(), cfg.getMinItemSize());
			} else if (cfg.getBiclustering().contains("qubic")) {
				algo = new QUBIC(dataModel, cfg.getConsistency(), cfg.getSize(), cfg.getOverlap(), cfg.getBin() ? this.threshold : null);
			} else if (cfg.getBiclustering().contains("xmotif")) {
				List<Interval> intervals = new ArrayList<Interval>(2);
				intervals.add(new Interval(Double.MIN_VALUE, true, this.threshold, false));
				intervals.add(new Interval(this.threshold, Double.MAX_VALUE));
				algo = new XMotif(dataModel, cfg.getNs(), cfg.getNd(), cfg.getSd(), intervals);
			} else if (cfg.getBiclustering().contains("random")) {
				algo = new RandomBiclustering(dataModel, cfg.getSize());
			} else if (cfg.getBiclustering().contains("coclust")) {
				algo = new COCLUSTBiclustering(dataModel, 10, 10, 30);
			} else {
				logger.error("Invalid biclustering algorithm {}", cfg.getBiclustering());
				return null;
			}
			
			logger.info("Running biclustering algorithm");
			algo.run();
			Biclustering<Long> biclusters = algo.get();
			logger.info("Done, got {} biclusters", biclusters.size());
			
			UserBiclusterSimilarity similarity = new JaccardUserBiclusterSimilarity(dataModel);
			UserBiclusterNeighborhood neighborhood = new NearestNBiclusterNeighborhood(cfg.getK(), similarity, dataModel, biclusters);
			
			return new NBCFRecommender(dataModel, neighborhood, similarity, s);
			
		} else if (this.config instanceof BBCFConfig) { /* BBCF */
			BBCFConfig cfg = (BBCFConfig) this.config;
			
			/* Biclustering algorithm choice */
			AbstractBiclusteringAlgorithm algo = null;
			if (cfg.getBiclustering().contains("bimax")) {
				algo = new Bimax(dataModel, cfg.getMinUserSize(), cfg.getMinItemSize());
			} else if (cfg.getBiclustering().contains("qubic")) {
				algo = new QUBIC(dataModel, cfg.getConsistency(), cfg.getSize(), cfg.getOverlap(), cfg.getBin() ? this.threshold : null);
			} else if (cfg.getBiclustering().contains("xmotif")) {
				List<Interval> intervals = new ArrayList<Interval>(2);
				intervals.add(new Interval(Double.MIN_VALUE, true, this.threshold, false));
				intervals.add(new Interval(this.threshold, Double.MAX_VALUE));
				algo = new XMotif(dataModel, cfg.getNs(), cfg.getNd(), cfg.getSd(), intervals);
			} else if (cfg.getBiclustering().contains("random")) {
				algo = new RandomBiclustering(dataModel, cfg.getSize());
			} else if (cfg.getBiclustering().contains("coclust")) {
				algo = new COCLUSTBiclustering(dataModel, 5, 5, 30);
			} else {
				logger.error("Invalid biclustering algorithm {}", cfg.getBiclustering());
				return null;
			}
			
			logger.info("Running biclustering algorithm");
			algo.run();
			Biclustering<Long> biclusters = algo.get();
			logger.info("Done, got {} biclusters", biclusters.size());
			
			UserBiclusterSimilarity similarity = new JaccardUserBiclusterSimilarity(dataModel);
			UserBiclusterNeighborhood neighborhood = new NearestNBiclusterNeighborhood(cfg.getK(), similarity, dataModel, biclusters);
			
			return new BBCFRecommender(dataModel, neighborhood, similarity, s);
			
		} else if (this.config instanceof BicaiNetConfig) { /* Bic-aiNet */
			BicaiNetConfig cfg = (BicaiNetConfig) this.config;
			
			AbstractBiclusteringAlgorithm algo = new BicaiNet(dataModel, cfg.getWr(), cfg.getWc(), cfg.getLambda(), cfg.getMaxNbIt(), cfg.getSupIt(), cfg.getO());
			logger.info("Running biclustering algorithm");
			algo.run();
			Biclustering<Long> biclusters = algo.get();
			logger.info("Done, got {} biclusters", biclusters.size());
			
			return new BicaiNetRecommender(dataModel, biclusters, s);
			
		} else {
			logger.error("Invalid / unimplemented recommender config type");
			return null;
		}
		
		
	}

}
