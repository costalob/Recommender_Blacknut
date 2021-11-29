/*
    Copyright 2020 Guillaume Longrais & Robinson Sablons De Gelis

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

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.common.FastByIDMap;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.model.GenericDataModel;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.model.PreferenceArray;

public class Grade {
    // The column number of each fields in the csv
    private static final int USER_ID = 1;
    private static final int GAME_ID = 6;
    private static final int DURATION = 4;
    private static final int STATUS = 5;

    BufferedReader br;

    Boolean header;

    String cvsSplitBy = ",";

    HashMap<String, HashMap<String, ArrayList<Integer>>> data;

    IdConversion IdConvert;

    public Grade(String path, Boolean header) {
        try {
            this.br = new BufferedReader(new FileReader(path));
            this.header = header;
            this.data = readData();
            this.IdConvert = new IdConversion();
            this.IdConvert.fillTables(this.data);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Grade(TableResult table) {
        HashMap<String, HashMap<String, ArrayList<Integer>>> userMap = new HashMap<>();

        for (FieldValueList row : table.iterateAll()) {
            String status = row.get("status").getStringValue();
            FieldValue durationValue = row.get("duration");
            if (status.equals("ended") && !durationValue.isNull()) {

                int duration;
                if (durationValue.getStringValue().equals("")) {
                    duration = 0;
                } else {
                    duration = (int) durationValue.getDoubleValue();
                }

                String userId = row.get("user").getStringValue();
                String gameId = row.get("game__global_id").getStringValue();
                if (userMap.containsKey(userId)) {
                    HashMap<String, ArrayList<Integer>> game = userMap.get(userId);

                    if (game.containsKey(gameId)) {
                        ArrayList<Integer> list = game.get(gameId);
                        list.add(duration);
                        game.put(gameId, list);
                    } else {
                        ArrayList<Integer> list = new ArrayList<Integer>();
                        list.add(duration);
                        game.put(gameId, list);
                    }

                } else {
                    ArrayList<Integer> list = new ArrayList<Integer>();
                    list.add(duration);

                    HashMap<String, ArrayList<Integer>> tmp = new HashMap<>();
                    tmp.put(gameId, list);

                    userMap.put(userId, tmp);
                }
            }


        }
        this.data = userMap;

        this.IdConvert = new IdConversion();
        this.IdConvert.fillTables(this.data);

    }

    /**
     * Calculate grades in relation to the total time a user has played a game
     * compared to other games
     * 
     * @return
     */
    public GenericDataModel AverageGameDuration() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<>();

        for (String user : this.data.keySet()) {
            HashMap<String, ArrayList<Integer>> tmp = this.data.get(user);

            int max = 0;
            HashMap<String, Integer> total_time = new HashMap<>();
            for (String game : tmp.keySet()) {
                ArrayList<Integer> g = tmp.get(game);
                int v = 0;
                for (int i : g) {
                    v += i;
                }

                total_time.put(game, v);

                if (v > max) {
                    max = v;
                }
            }

            if (max <= 0) {
                continue;
            }

            PreferenceArray pref = new GenericUserPreferenceArray(tmp.size());

            pref.setUserID(0, IdConvert.getNewUserId(user));

            DecimalFormat df = new DecimalFormat("#.#");
            int index = 0;
            for (String game : total_time.keySet()) {
                float sessionTime = (float) total_time.get(game);
                if (sessionTime > 0.0f) {
                    pref.setItemID(index, IdConvert.getNewGameId(game));
                    pref.setValue(index, Float.parseFloat(df.format(10.0f * (sessionTime / (float) max))));
                    index++;
                }
            }

            grades.put(IdConvert.getNewUserId(user), pref);
        }

        return new GenericDataModel(grades);
    }

    /**
     * Calculate grades in relation to the average duration of a session for each
     * games of a user
     * 
     * @return
     */
    public GenericDataModel AverageGameSession() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<PreferenceArray>();

        Set<String> idGame = IdConvert.getAllOriginalGameId();
        HashMap<String, Integer> averageTimeSession = new HashMap<>();

        for (String game : idGame) {
            int average = 0;
            int counter = 0;
            for (String user : data.keySet()) {
                HashMap<String, ArrayList<Integer>> games = data.get(user);
                if (games.containsKey(game)) {
                    ArrayList<Integer> session = games.get(game);
                    for (int v : session) {
                        average += v;
                        counter++;
                    }
                }
            }
            averageTimeSession.put(game, average / counter);
        }

        for (String user : this.data.keySet()) {
            HashMap<String, ArrayList<Integer>> tmp = this.data.get(user);

            PreferenceArray pref = new GenericUserPreferenceArray(tmp.size());
            pref.setUserID(0, IdConvert.getNewUserId(user));
            DecimalFormat df = new DecimalFormat("#.#");
            int index = 0;

            for (String game : tmp.keySet()) {
                float average = (float) averageTimeSession.get(game);
                ArrayList<Integer> sessions = tmp.get(game);
                float grade = 0.0f;

                for (int session : sessions) {
                    float tmpGrade = (float) session / (average * 1.3f);
                    if (tmpGrade > 1.0f) {
                        tmpGrade = 1;
                    }
                    grade += tmpGrade;
                }

                grade /= (float) sessions.size();
                grade *= 10.0f;

                if (grade == 0.0f) {
                    continue;
                }

                pref.setItemID(index, IdConvert.getNewGameId(game));
                pref.setValue(index, Float.parseFloat(df.format(grade)));
                index++;

            }

            if (index == 0) {
                continue;
            }

            grades.put(IdConvert.getNewUserId(user), pref);
        }

        return new GenericDataModel(grades);
    }

    /**
     * Calculate grades hybrid with AverageGameSession and AverageGameDuration
     * 
     * @return
     */
    public GenericDataModel HybridDurationSession() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<PreferenceArray>();

        DataModel duration = this.AverageGameDuration();
        DataModel session = this.AverageGameSession();

        try {
            LongPrimitiveIterator it_user = duration.getUserIDs();
            while (it_user.hasNext()) {
                long id_user = it_user.next();

                PreferenceArray pref = new GenericUserPreferenceArray(
                        duration.getPreferencesFromUser(id_user).length());
                pref.setUserID(0, id_user);
                DecimalFormat df = new DecimalFormat("#.#");

                FastIDSet games = duration.getItemIDsFromUser(id_user);
                LongPrimitiveIterator it_game = games.iterator();

                int index = 0;
                while (it_game.hasNext()) {
                    long id_game = it_game.next();
                    pref.setItemID(index, id_game);
                    pref.setValue(index, Float.parseFloat(df.format((duration.getPreferenceValue(id_user, id_game)
                            + session.getPreferenceValue(id_user, id_game)) / 2)));
                    index++;
                }
                
                grades.put(id_user, pref);
            }
        } catch (TasteException e) {
            e.printStackTrace();
        }

        return new GenericDataModel(grades);
    }

    /**
     * Calculate grades in a binary way
     * 
     * @return
     */
    public GenericDataModel Binary() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<>();

        for (String user : this.data.keySet()) {
            HashMap<String, ArrayList<Integer>> tmp = this.data.get(user);

            PreferenceArray pref = new GenericUserPreferenceArray(tmp.size());
            pref.setUserID(0, IdConvert.getNewUserId(user));
            DecimalFormat df = new DecimalFormat("#.#");
            int index = 0;
            for (String game : tmp.keySet()) {
                ArrayList<Integer> sessions = tmp.get(game);
                boolean notZero = false;
                for (int session : sessions) {
                    if (session > 0) {
                        notZero = true;
                    }
                }
                if (!notZero) {
                    continue;
                }
                pref.setItemID(index, IdConvert.getNewGameId(game));
                pref.setValue(index, Float.parseFloat(df.format(10)));
                index++;

            }
            if (index == 0) {
                continue;
            }
            grades.put(IdConvert.getNewUserId(user), pref);
        }

        return new GenericDataModel(grades);
    }

    /**
     * Calculate grades in relation to the number of time a user has played a game
     * compared to other games
     * 
     * @return
     */
    public GenericDataModel NumberOfSession() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<>();

        for (String user : this.data.keySet()) {
            HashMap<String, ArrayList<Integer>> tmp = this.data.get(user);
            int max = 0;
            HashMap<String, Integer> total_time = new HashMap<>();
            for (String game : tmp.keySet()) {
                ArrayList<Integer> g = tmp.get(game);
                int v = g.size();
                
       
                total_time.put(game, v);

                if (v > max) {
                    max = v;
                    
                }
            }
            PreferenceArray pref = new GenericUserPreferenceArray(tmp.size());

            pref.setUserID(0, IdConvert.getNewUserId(user));

            DecimalFormat df = new DecimalFormat("#.#");
            int index = 0;
            for (String game : total_time.keySet()) {
                ArrayList<Integer> sessions = tmp.get(game);
              
                boolean notZero = false;
                for (int session : sessions) {
                    if (session > 0) {
                        notZero = true;
                    }
                }
                if (!notZero) {
                    continue;
                }
                float sessionTime = (float) total_time.get(game);
            
                if (sessionTime > 0.0f) {
                    pref.setItemID(index, IdConvert.getNewGameId(game));
                    pref.setValue(index, Float.parseFloat(df.format(10.0f * (sessionTime / (float) max))));
                   
                    index++;
                }
            }
            if (index == 0) {
                continue;
            }
            grades.put(IdConvert.getNewUserId(user), pref);
        }
        return new GenericDataModel(grades);
    }

    /**
     * Calculate grades hybrid with NumberOfSession and AverageGameDuration
     * 
     * @return
     */
    public GenericDataModel HybridDurationNumber() {
        FastByIDMap<PreferenceArray> grades = new FastByIDMap<PreferenceArray>();

        DataModel duration = this.AverageGameDuration();
        DataModel number = this.NumberOfSession();

        try {
            LongPrimitiveIterator it_user = duration.getUserIDs();
            while (it_user.hasNext()) {
                long id_user = it_user.next();

                PreferenceArray pref = new GenericUserPreferenceArray(
                        duration.getPreferencesFromUser(id_user).length());
                pref.setUserID(0, id_user);
                DecimalFormat df = new DecimalFormat("#.#");

                FastIDSet games = duration.getItemIDsFromUser(id_user);
                LongPrimitiveIterator it_game = games.iterator();

                int index = 0;
                while (it_game.hasNext()) {
                    long id_game = it_game.next();
                    pref.setItemID(index, id_game);
                    pref.setValue(index, Float.parseFloat(df.format((duration.getPreferenceValue(id_user, id_game)
                            + number.getPreferenceValue(id_user, id_game)) / 2)));
                    index++;
                }
                grades.put(id_user, pref);
            }
        } catch (TasteException e) {
            e.printStackTrace();
        }

        return new GenericDataModel(grades);
    }

    public void toCSV(DataModel dm, String fileName) {
        try {
            LongPrimitiveIterator it_user = dm.getUserIDs();
            PrintWriter writer = new PrintWriter(fileName);
            while (it_user.hasNext()) {
                long id_user = it_user.next();
                FastIDSet games = dm.getItemIDsFromUser(id_user);
                LongPrimitiveIterator it_game = games.iterator();
                while (it_game.hasNext()) {
                    long id_game = it_game.next();
                    writer.println(id_user + "," + id_game + "," + dm.getPreferenceValue(id_user, id_game));
                }
            }
            writer.close();
        } catch (TasteException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String getOldGameId(int id) {
        return IdConvert.getOriginalGameId(id);
    }

    public String getOldUserId(int id) {
        return IdConvert.getOriginalUserId(id);
    }

    private HashMap<String, HashMap<String, ArrayList<Integer>>> readData() throws IOException {
        HashMap<String, HashMap<String, ArrayList<Integer>>> userMap = new HashMap<>();

        String line = "";

        if (header) {
            this.br.readLine();
        }

        while ((line = this.br.readLine()) != null) {
            String[] data = line.split(cvsSplitBy);

            if (data[STATUS].equals("ended")) {
                if (data[DURATION].equals("")) {
                    data[DURATION] = "0";
                }
                if (userMap.containsKey(data[USER_ID])) {
                    HashMap<String, ArrayList<Integer>> game = userMap.get(data[USER_ID]);

                    if (game.containsKey(data[GAME_ID])) {
                        ArrayList<Integer> list = game.get(data[GAME_ID]);
                        list.add(Integer.valueOf(data[DURATION]));
                        game.put(data[GAME_ID], list);
                    } else {
                        ArrayList<Integer> list = new ArrayList<Integer>();
                        list.add(Integer.valueOf(data[DURATION]));
                        game.put(data[GAME_ID], list);
                    }

                } else {
                    ArrayList<Integer> list = new ArrayList<Integer>();
                    list.add(Integer.valueOf(data[DURATION]));

                    HashMap<String, ArrayList<Integer>> tmp = new HashMap<>();
                    tmp.put(data[GAME_ID], list);

                    userMap.put(data[USER_ID], tmp);
                }
            }
        }

        return userMap;
    }
}
