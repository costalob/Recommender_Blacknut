package binder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.PredictionStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.impl.common.DataPreprocessing;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.cf.taste.impl.eval.Fold;
import org.apache.mahout.cf.taste.impl.eval.KFoldDataSplitter;
import org.apache.mahout.cf.taste.impl.eval.KFoldRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.KFoldRecommenderPerUserEvaluator;
import org.apache.mahout.cf.taste.impl.eval.KFoldRecommenderPredictionEvaluator;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.google.api.services.bigquery.Bigquery.Datasets.List;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

import binder.config.AbstractConfig;
import binder.config.BBCFConfig;
import binder.config.BCNConfig;
import binder.config.BicaiNetConfig;
import binder.config.COCLUSTConfig;
import binder.config.COCLUSTRConfig;
import binder.config.Config;
import binder.config.IBKNNConfig;
import binder.config.ItemAvgConfig;
import binder.config.ItemUserAvgConfig;
import binder.config.MFConfig;
import binder.config.NBCFConfig;
import binder.config.RandomConfig;
import binder.config.UBKNNConfig;

public class Calculator {
	
	private static Logger logcfg = LoggerFactory.getLogger("Config");
	private static Logger logr = LoggerFactory.getLogger("Pred");
	private static Logger logt = LoggerFactory.getLogger("Time");

	private static Logger logir = LoggerFactory.getLogger("IRStats");
	static String prefix = "src/main/resources/";
	private static Logger logger = LoggerFactory.getLogger(Calculator.class);
	
	public static void main(String[] args)throws IOException, TasteException{
		
		Locale locale = new Locale("en_US"); 
		Locale.setDefault(locale);

		String cfgFileName = prefix + "default_config.yml";

		/* Check command line arguments */
		CommandLineParser parser = new DefaultParser();
		Options options = new Options();
		options.addOption("c", "config", true, "path of config file, otherwise default used");
		try {
			CommandLine line = parser.parse(options, args);
			if (line.hasOption("config")) {
				cfgFileName = line.getOptionValue("config");
			}
		} catch (ParseException exp) {
			exp.printStackTrace();
			System.exit(1);
		}

		/* Load configuration file */
		logger.info("Using {} as configuration file", cfgFileName);
		Config cfg = null;
		Yaml yaml = new Yaml();
		try (InputStream in = Files.newInputStream(Paths.get(cfgFileName))) {
			cfg = yaml.loadAs(in, Config.class);
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("Couldn't read main configuration file");
			System.exit(1);
		}

		if(cfg.getNbUserPerFile() < 0){
			logger.error("The number of users per file can't be negative");
			System.exit(1);
		}

		logger.info("=== MAIN CONFIGURATION ===");
		cfg.logConfig(logger);

		/* Load dataset */

		logger.info("Loading dataset");

		Grade g = null;;

		ArrayList<String> games = new ArrayList<>();

		if (cfg.getData().equals("online")) {

			String projectId = "blacknut-analytics";
			File credentialsPath = new File(cfg.getKeyPath());

			// Load credentials from JSON key file.

			GoogleCredentials credentials;
			try (FileInputStream serviceAccountStream = new FileInputStream(credentialsPath)) {
				
				credentials = ServiceAccountCredentials.fromStream(serviceAccountStream);
				
			}

			// Instantiate a client.

			BigQuery bigquery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(projectId).build()
					.getService();
			
			System.out.println("loading.....");

			QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder("SELECT * from external_share.streams")
					.setUseLegacySql(false).build();
			QueryJobConfiguration queryConfigGames = QueryJobConfiguration.newBuilder("SELECT * from external_share.games")
					.setUseLegacySql(false).build();
			
			System.out.println("wait...");

			JobId jobId = JobId.of(UUID.randomUUID().toString());
			JobId jobIdGames = JobId.of(UUID.randomUUID().toString());
			Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
			Job queryJobGames = bigquery.create(JobInfo.newBuilder(queryConfigGames).setJobId(jobIdGames).build());

			// Wait for the query to complete.

			try {
				queryJob = queryJob.waitFor();
				queryJobGames = queryJobGames.waitFor();
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				System.exit(1);
			}

			TableResult result;
			TableResult gamesTable = null;
			try {

				result = queryJob.getQueryResults();
				gamesTable = queryJobGames.getQueryResults();
				g = new Grade(result);
			} catch (JobException e1) {
				e1.printStackTrace();
				System.exit(1);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
				System.exit(1);
			}

			for (FieldValueList row : gamesTable.iterateAll()) {
				games.add(row.get("global_id").getStringValue());
			}
		} else if (cfg.getData().equals("local")) {
			g = new Grade(cfg.getDataset(), true);
		} else {
			logger.error("Data config is not set local or online");
			System.exit(1);
		}

		DataModel model = null;
		model = g.NumberOfSession();
		

		if (cfg.getNormalize()) {
			try {
				logger.info("Normalizing dataset");
				model = DataPreprocessing.normalize(model);
			} catch (TasteException e) {
				e.printStackTrace();
				logger.error("Couldn't normalize dataset");
				System.exit(1);
			}
		}
		if (cfg.getBinarize()) {
			try {
				logger.info("Binarizing dataset");
				model = DataPreprocessing.binarize(model, 3.0f);
			} catch (TasteException e) {
				e.printStackTrace();
				logger.error("Couldn't binarize dataset");
				System.exit(1);
			}
		}
		logger.info("Done with dataset");
		
		/* Read configuration files of all recommender algorithms specified */
		HashMap<String, AbstractConfig> configs = new HashMap<String, AbstractConfig>(cfg.getConfigs().size());
		for (String s : cfg.getConfigs()) {
			AbstractConfig c = null;
			Yaml yml = new Yaml();
			if (s.equals("random")) {
				c = new RandomConfig();
			} else if (s.equals("itemavg")) {
				c = new ItemAvgConfig();
			} else if (s.equals("itemuseravg")) {
				c = new ItemUserAvgConfig();
			} else {
				try (InputStream in = Files.newInputStream(Paths.get(prefix + s))) {
					if (s.contains("ubknn")) {
						c = yml.loadAs(in, UBKNNConfig.class);
					} else if (s.contains("ibknn")) {
						c = yml.loadAs(in, IBKNNConfig.class);
					} else if (s.contains("mf")) {
						c = yml.loadAs(in, MFConfig.class);
					} else if (s.contains("coclustr")) {
						c = yml.loadAs(in, COCLUSTRConfig.class);
					} else if (s.contains("coclust")) {
						c = yml.loadAs(in, COCLUSTConfig.class);
					} else if (s.contains("nbcf")) {
						c = yml.loadAs(in, NBCFConfig.class);
					} else if (s.contains("bbcf")) {
						c = yml.loadAs(in, BBCFConfig.class);
					} else if (s.contains("bicainet")) {
						c = yml.loadAs(in, BicaiNetConfig.class);
					} else if (s.contains("bcn")) {
						c = yml.loadAs(in, BCNConfig.class);
					} else {
						logger.error("Unrecognized algorithm");
						return;
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error("Couldn't read specific configuration file {}", s);
					return;
				}
			}
			configs.put(s, c);
		}
		/* Run the evaluation */
		System.out.println("starting recommendations");
		
		
		try {
			System.out.println(cfg.getFolds());
			KFoldRecommenderPredictionEvaluator evaluatorPred = new KFoldRecommenderPredictionEvaluator(model,
					cfg.getFolds());
			KFoldRecommenderIRStatsEvaluator evaluatorIRStats = new KFoldRecommenderIRStatsEvaluator(model,
					cfg.getFolds());
			KFoldRecommenderPerUserEvaluator evaluatorPerUser = new KFoldRecommenderPerUserEvaluator(model,
					cfg.getFolds());

			if (cfg.getFilterBBCF()) {
				BBCFConfig cc = new BBCFConfig();
				cc.setBiclustering("qubic");
				cc.setConsistency((float) 0.95);
				cc.setSize(100);
				cc.setOverlap(1);
				cc.setK(20);
				evaluatorIRStats.restrainUserIDsWithCoverage(
						new BinderRecommenderBuilder(cc, "trainingitems", cfg.getThreshold()), cfg.getR());
			}

			Iterator<Entry<String, AbstractConfig>> it = configs.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, AbstractConfig> pair = it.next();
				logcfg.info("=== SPECIFIC CONFIGURATION #{} ===", pair.getKey());
				AbstractConfig c = (AbstractConfig) pair.getValue();
				c.logConfig(logcfg);
				String name = pair.getKey();
				for (String strategy : cfg.getStrategies()) {
					RecommenderBuilder builder = new BinderRecommenderBuilder(c, strategy, cfg.getThreshold());
					if (cfg.getDoPredEval()) {
						runPredEval(evaluatorPred, builder, cfg, name);
					}
					if (cfg.getDoIREval()) {
						runIREval(evaluatorIRStats, builder, model, cfg, name,
								strategy + String.valueOf(cfg.getThreshold()));
					}
					if (cfg.getDoTimeEvel()) {
						runTimeEval(builder, model, cfg, name, strategy + String.valueOf(cfg.getThreshold()));
					}
					
				}
			}
		} catch (TasteException e) {
			e.printStackTrace();
			logger.error("Error during evaluation");
			return;
		}
	}
	
		
	
	
	private static void runIREval(KFoldRecommenderIRStatsEvaluator evaluatorIRStats, RecommenderBuilder builder,
			DataModel model, Config cfg, String name, String strategy) throws TasteException {
		System.out.println("a");
		IRStatistics irstats = null;
		for (int k = 0; k < cfg.getNruns(); k++) {
			for (int R = cfg.getOneR() ? cfg.getR() : 5; R <= cfg.getR(); R += 5) {
				
				irstats = evaluatorIRStats.evaluate(builder, R, (double) (cfg.getBinarize() ? 1 : cfg.getThreshold()));
				logir.info("{},{},{},{},{},{},{},{},{},{},{},-1,{},{},{}", name, R, irstats.getPrecision(),
						irstats.getRecall(), irstats.getF1Measure(), irstats.getReachAtLeastOne(),
						irstats.getNormalizedDiscountedCumulativeGain(), irstats.getFallOut(), irstats.getReachAll(),
						irstats.getItemCoverage(), strategy, irstats.getPerPrecision(), irstats.getPerRecall(),
						irstats.getDiversity());
			}
		}
	}
	
	static class TimeFunc {
		static long call() {
			return System.nanoTime();
//	    	return System.currentTimeMillis();
		}
	}

	private static void runTimeEval(RecommenderBuilder builder, DataModel model, Config cfg, String name,
			String strategy) throws TasteException {

		for (int k = 0; k < cfg.getNruns(); k++) {
			KFoldDataSplitter folds = new KFoldDataSplitter(model, cfg.getFolds());
			for (int R = cfg.getOneR() ? cfg.getR() : 5; R <= cfg.getR(); R += 5) {
				
				RunningAverage trainTime = new FullRunningAverage();
				RunningAverage predPerUserTime = new FullRunningAverage();
				
				Iterator<Fold> itF = folds.getFolds();
				while (itF.hasNext()) {

					Fold fold = itF.next();
					DataModel trainingModel = fold.getTraining();
					long t1, t2;

					t1 = TimeFunc.call();
					Recommender recommender = builder.buildRecommender(trainingModel, fold);
					t2 = TimeFunc.call();
					trainTime.addDatum(t2 - t1);

					LongPrimitiveIterator it = model.getUserIDs();
					int cnt = 0;
					double sum = 0.0;
					while (it.hasNext()) {
						long userID = it.nextLong();
						
						try {
							t1 = TimeFunc.call();
							recommender.recommend(userID, R);
							t2 = TimeFunc.call();
							sum += t2 - t1;
							cnt++;
						} catch (NoSuchUserException nsue) {
							continue;
						} catch (NoSuchItemException nsie) {
							continue;
						}
						
					}
					predPerUserTime.addDatum(sum / (double) cnt);
				}
				logt.info("{},{},{},{},{}", name, R, trainTime.getAverage(), predPerUserTime.getAverage(), strategy);
			}
		}
	}
		
	private static void runPredEval(KFoldRecommenderPredictionEvaluator evaluator, RecommenderBuilder builder,
			Config cfg, String name) throws TasteException {
		for (int k = 0; k < cfg.getNruns(); k++) {
			PredictionStatistics results = evaluator.evaluate(builder);
			logr.info("{},{},{},{}", name, results.getRMSE(), results.getMAE(), results.getNoEstimate());
		}
	}
		

	}


