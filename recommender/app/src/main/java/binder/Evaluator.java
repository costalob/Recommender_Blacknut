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

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.io.File;


import org.yaml.snakeyaml.Yaml;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;



import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.impl.common.DataPreprocessing;
import org.apache.mahout.cf.taste.impl.common.FullRunningAverage;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.common.RunningAverage;
import org.apache.mahout.cf.taste.impl.recommender.AdaptativeCOCLUSTRecommender;
import org.apache.mahout.cf.taste.impl.recommender.BBCFRecommender;
import org.apache.mahout.cf.taste.impl.recommender.BCNRecommender;
import org.apache.mahout.cf.taste.impl.recommender.BicaiNetRecommender;
import org.apache.mahout.cf.taste.impl.recommender.COCLUSTRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.ItemUserAverageRecommender;
import org.apache.mahout.cf.taste.impl.recommender.NBCFRecommender;
import org.apache.mahout.cf.taste.impl.recommender.RandomRecommender;
import org.apache.mahout.cf.taste.impl.recommender.svd.SVDRecommender;
import org.apache.mahout.cf.taste.common.NoSuchItemException;
import org.apache.mahout.cf.taste.common.NoSuchUserException;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
import com.google.gson.JsonArray;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Evaluator {

	static String prefix = "src/main/resources/";
	//private static Logger logger = LoggerFactory.getLogger(Evaluator.class);
	private static final Logger logger = LogManager.getLogger(Evaluator.class);
	private static RandomRecommender random;
	private static GenericUserBasedRecommender ubknn;
	private static ItemAverageRecommender itemavg;
	private static SVDRecommender mf;
	private static ItemUserAverageRecommender itemuseravg;
	private static GenericItemBasedRecommender ibknn;
	private static COCLUSTRecommender coclust;
	private static COCLUSTRecommender coclustr;
	private static NBCFRecommender nbcf;
	private static BBCFRecommender bbcf;
	private static BicaiNetRecommender bicainet;
	private static BCNRecommender bcn;

	public static void main(String[] args) throws IOException, TasteException, org.json.simple.parser.ParseException {
		
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
		System.out.println(model.getNumItems());
		

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

		/* Read configuration files of all recommender algorithms specified and train the model */

		HashMap<String, AbstractConfig> configs = new HashMap<String, AbstractConfig>(cfg.getConfigs().size());
		for (String s : cfg.getConfigs()) {
			AbstractConfig c = null;
			Yaml yml = new Yaml();
			if (s.equals("random")) {
				c = new RandomConfig();
				RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
				random = (RandomRecommender) builder.buildRecommender(model);
				//runTimeEval(builder,model,c,"random",cfg);
				
			} else if (s.equals("itemavg")) {
				c = new ItemAvgConfig();
				RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
				itemavg = (ItemAverageRecommender) builder.buildRecommender(model);
				//runTimeEval(builder,model,c,"itemavg",cfg);
				
			} else if (s.equals("itemuseravg")) {
				c = new ItemUserAvgConfig();
				RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
				itemuseravg = (ItemUserAverageRecommender) builder.buildRecommender(model);
				//runTimeEval(builder,model,c,"itemuseravg",cfg);
				
			} else {
				try (InputStream in = Files.newInputStream(Paths.get(prefix + s))) {
					if (s.contains("ubknn")) {
						c = yml.loadAs(in, UBKNNConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						ubknn = (GenericUserBasedRecommender) builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"ubknn",cfg);
						
					} else if (s.contains("ibknn")) {
					
						c = yml.loadAs(in, IBKNNConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						ibknn = (GenericItemBasedRecommender) builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"ibknn",cfg);
						
					} else if (s.contains("mf")) {
						c = yml.loadAs(in, MFConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						mf = (SVDRecommender) builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"mf",cfg);
					} else if (s.contains("coclustr")) {
						c = yml.loadAs(in, COCLUSTRConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						coclustr = (COCLUSTRecommender)builder.buildRecommender(model);
					} else if (s.contains("coclust")) {
						c = yml.loadAs(in, COCLUSTConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						coclust = (COCLUSTRecommender)builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"coclust",cfg);
						
					} else if (s.contains("nbcf")) {
						c = yml.loadAs(in, NBCFConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						nbcf = (NBCFRecommender)builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"nbcf",cfg);
						
					} else if (s.contains("bbcf")) {
						c = yml.loadAs(in, BBCFConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						bbcf = (BBCFRecommender)builder.buildRecommender(model);
						//runTimeEval(builder,model,c,"bbcf",cfg);
						
					} else if (s.contains("bicainet")) {
						c = yml.loadAs(in, BicaiNetConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						//bicainet =(BicaiNetRecommender)builder.buildRecommender(model);
						
						
					} else if (s.contains("bcn")) {
						c = yml.loadAs(in, BCNConfig.class);
						RecommenderBuilder builder = new BinderRecommenderBuilder(c, 3.0f);
						bcn =(BCNRecommender)builder.buildRecommender(model);
						
					} else {
						logger.error("Unrecognized algorithm");
						System.exit(1);
					}
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.error("Couldn't read specific configuration file {}", s);
					System.exit(1);
				}
			}
			configs.put(s, c);
		}
		
		//AssignAlgo
	
			
	         File file = new File(prefix+"user_algo.json");
	         if (!file.exists()) {
	        	 
	        	 assignAlgoToUser(model,g,cfg.getConfigs());
	        	 
	         }
	         
		
		
		///// 

		/* Read configuration files of all tests specified,
		 * 13/11/2021 it don't know what it does it might be removed */

		/*JSONParser jsonParser = new JSONParser();
		JSONObject currentTest = null;
         
        try (FileReader reader = new FileReader(cfg.getTestPath()))
        {
            //Read JSON file
			Object obj = jsonParser.parse(reader);
			DateFormat format = new SimpleDateFormat("dd_MM_yyyy");

			// changing the date just for testing
			//Date today = new Date();
			Date today = format.parse("10_08_2020");
			Date tmpDate = null;
 
			String endDateString = (String) ((JSONObject)obj).get("end_date");
			Date endDate = format.parse(endDateString);

			if(today.after(endDate)){
				logger.error("Test period completed : {}", endDate);
				System.exit(1);
			}

			JSONArray tests = (JSONArray) ((JSONObject)obj).get("tests");

			for(Object test : tests){
				Date d = format.parse((String) ((JSONObject)test).get("start_date"));
				if(tmpDate == null){
					tmpDate = d;
					currentTest = (JSONObject) test;
				}

				if(d.compareTo(tmpDate) > 0 && d.compareTo(today) <= 0){
					tmpDate = d;
					currentTest = (JSONObject) test;
				}
			}

			if(tmpDate == null || tmpDate.after(today)){
				logger.error("No tests to do today : {}", today);
				System.exit(1);
			}
             
 
        } catch (FileNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
        } catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
        }  catch (org.json.simple.parser.ParseException e) {
			e.printStackTrace();
			System.exit(1);
		} catch (java.text.ParseException e) {
			e.printStackTrace();
			System.exit(1);
		}*/

		/* Recommendations */
	    Object obj = new JSONParser().parse(new FileReader(prefix+"user_algo.json"));
        
		try {
			logger.info("Starting recommendations");

			
			JSONObject results= new JSONObject();
			JSONArray users = new JSONArray(); 
			JSONArray reco = new JSONArray();
			
			FileWriter f;
			LongPrimitiveIterator it_user = model.getUserIDs();
			int numUser = model.getNumUsers();
			int numRec = cfg.getNbRecommendation();
			int nbFile = 0;
			if(cfg.getNbUserPerFile() != 0){
				nbFile = numUser/cfg.getNbUserPerFile() + 1;
			}
			int fileNb = 1;
			int index = 0;
			
			

			while (it_user.hasNext()) {
				
				long id = it_user.next();

				
				if (cfg.getNbUserPerFile() > 0 && cfg.getNbUserPerFile() == index) {
					
					String str = cfg.getResultPath();
					f = new FileWriter(str.substring(0, str.length() - 5) + fileNb + "_" + nbFile + ".json");
					f.write(results.toJSONString());
					f.flush();
					f.close();
					reco=new JSONArray();
					users = new JSONArray();
					results= new JSONObject();
					fileNb++;
					index=0;
				}

				JSONObject user = new JSONObject();
				
			
				reco= new JSONArray();
				
				String id_old = g.getOldUserId((int) id);
				user.put("user_id", id_old);
				
				Iterator<Entry<String, AbstractConfig>> it = configs.entrySet().iterator();
			
				while (it.hasNext()) {
					Entry<String, AbstractConfig> pair = it.next();
					AbstractConfig c = (AbstractConfig) pair.getValue();
					// c.logConfig(logcfg);
					
					String name = pair.getKey();
					//System.out.println(name);
					
					List<RecommendedItem> itemRecommendations = null;
					
					if(name.equals("random")) {
						itemRecommendations = random.recommend(id,numRec );
					}
					else if(name.contains("ubknn")) {
						itemRecommendations = ubknn.recommend(id, numRec);
					}
					else if (name.equals("itemavg")) {
						itemRecommendations = itemavg.recommend(id, numRec);
					}
					else if (name.contains("mf")) {
						itemRecommendations = mf.recommend(id, numRec);
					}
					else if (name.contains("itemuseravg")) {
						itemRecommendations = itemuseravg.recommend(id, numRec);
					}
					else if (name.contains("ibknn")) {
						itemRecommendations = ibknn.recommend(id, numRec);
						
					}else if (name.contains("coclust")) {
						itemRecommendations = ((Recommender) coclust).recommend(id, numRec);
					}
					else if (name.contains("nbcf")) {
						itemRecommendations = nbcf.recommend(id,numRec);	
					}
					else if (name.contains("bbcf")) {
						itemRecommendations = bbcf.recommend(id, numRec);
					}
					else if (name.contains("bicainet")) {
						itemRecommendations = bicainet.recommend(id, numRec);
					}
					else if(name.contains("bcn")) {
						itemRecommendations = bcn.recommend(id, numRec);
					}
					else if (name.contains("coclustr")) {
						itemRecommendations = ((Recommender) coclustr).recommend(id, numRec);
					}
					
		
					
					
					JSONObject element_of_reco = new JSONObject();
					JsonArray game_ids = new JsonArray();
					
					
					for (RecommendedItem itemRecommendation : itemRecommendations) {
						String idGame = g.getOldGameId((int) itemRecommendation.getItemID());
						if(cfg.getData().equals("local") || games.contains(idGame)){
							game_ids.add(idGame);
							
						}
					}
						
					element_of_reco.put("game_ids", game_ids);
					element_of_reco.put("algorithm_name", name);
					reco.add(element_of_reco);
					
				}
				
				// todo how to assign the algorithm to a right person, what to display? 
				
				// call get algo to show function
		
				/*JSONArray algos = (JSONArray) currentTest.get("algos");
				Random rand = new Random();
				int i = rand.nextInt(algos.size());*/

				String algo = getAlgotoDisplay(id_old,obj);
				if (algo == null) {
					
					assignNewUser(id_old,cfg.getConfigs());
				}
				user.put("display", algo);
				user.put("recommendations", reco);
				users.add(user);
				results.put("results",users);
				index++;

			}
			if (cfg.getNbUserPerFile() == 0) {
				f = new FileWriter(cfg.getResultPath());
			} else if (index != 0) {
				f = new FileWriter("result" + fileNb + "_" + nbFile + ".json");
			} else {
				System.exit(1);
				return;
			}
			
			f.write(results.toJSONString());
			f.flush();
			f.close();

			logger.info("Recommendations finished");
			
			

		} catch (TasteException e) {
			e.printStackTrace();
			logger.error("Error during recommendations");
			System.exit(1);
		}

		System.exit(0);

	}
	static class TimeFunc {
		static long call() {
			return System.nanoTime();
//	    	return System.currentTimeMillis();
		}
	}
	
	
	private static void runTimeEval(RecommenderBuilder builder, DataModel model, AbstractConfig c, String name, Config cfg) throws TasteException, IOException {
		RunningAverage trainTime = new FullRunningAverage();
		RunningAverage predPerUserTime = new FullRunningAverage();
		long t1,t2;
		
		t1 = TimeFunc.call();
		Recommender recommender = builder.buildRecommender(model);
		t2 = TimeFunc.call();
		trainTime.addDatum((t2 - t1)/1000000000);
		
		LongPrimitiveIterator it = model.getUserIDs();
		int cnt = 0;
		double sum = 0.0;
		while(it.hasNext()) {
			
			long userID = it.nextLong();
			
			try {
				t1 = TimeFunc.call();
				recommender.recommend(userID, 5);
				t2 = TimeFunc.call();
				sum += t2 - t1;
				cnt++;
			} catch (NoSuchUserException nsue) {
				continue;
			} catch (NoSuchItemException nsie) {
				continue;
			}
			
		}
		predPerUserTime.addDatum((sum / (double) cnt)/1000000);
		String str = "src/main/resources/timeResult/result.json";
		FileWriter f = new FileWriter(str.substring(0, str.length() - 5) + name + ".txt");
		f.write("algo name: "+ name);
		f.write("\n");
		f.write("this is the training time in seconds : "+ trainTime.toString());
		f.write("\n");
		f.write("avg prediction time per user in milliseconds : "+ predPerUserTime.toString());
		f.flush();
		f.close();
		logger.info("done time evuation");
		
		
	}
	
	// this method is called when there is no assigner file on the disk
	
	
	public static void assignAlgoToUser(DataModel model, Grade g,List<String> configs) throws TasteException, IOException {
		
		FileWriter f = new FileWriter(prefix+"user_algo.json");
		LongPrimitiveIterator it_user = model.getUserIDs();
		JSONObject results= new JSONObject();
		JSONArray users = new JSONArray(); 
		
		while (it_user.hasNext()) {
			Random rand = new Random();
			int i = rand.nextInt(configs.size());
			String algo = configs.get(i);
			long id = it_user.next();
			results.put(g.getOldUserId((int) id),algo);
		}
		
		
		f.write(results.toJSONString());
		f.flush();
		f.close();
		
		
	}
	
	private static String cleaner(String name) {
		
		
		if(name.equals("random")) {
			return "random";
		}
		else if(name.contains("ubknn")) {
			return "ubknn";
		}
		else if (name.equals("itemavg")) {
			return "itemavg";
		}
		else if (name.contains("mf")) {
			return "mf";	
		}
		else if (name.contains("itemuseravg")) {
			return "itemuseravg";
		}
		else if (name.contains("ibknn")) {
			return "ibknn";
			
		}else if (name.contains("coclust")) {
			return "coclust";
		}
		else if (name.contains("nbcf")) {
			return "nbcf";
		}
		else if (name.contains("bbcf")) {
			return "bbcf";
		}
		else if (name.contains("bicainet")) {
			return "bicainet";
		}
		else if(name.contains("bcn")) {
			return "bcn";
		}
		else if (name.contains("coclustr")) {
			return "coclustr";
		}
		
		return "empty";
		
		
		
	}
	
     
	
	private static String getAlgotoDisplay(String username, Object obj) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
		
		
		//Object obj = new JSONParser().parse(new FileReader(prefix+"user_algo.json"));
        JSONObject jo = (JSONObject) obj;
        return (String) jo.get(username);
    
	}
	
	
	// this function takes a long time but I don't know how to improve it
	private static void assignNewUser(String id_old,List<String> configs) throws FileNotFoundException, IOException, org.json.simple.parser.ParseException {
		
		
		JSONObject obj =(JSONObject) new JSONParser().parse(new FileReader(prefix+"user_algo.json"));
		Random rand = new Random();
		int i = rand.nextInt(configs.size());
		String algo = configs.get(i);
		obj.put(id_old,algo);
		FileWriter file = new FileWriter(prefix+"user_algo.json");
        file.write(obj.toJSONString());
        file.flush();
		
		
	}
	
	
	

}
