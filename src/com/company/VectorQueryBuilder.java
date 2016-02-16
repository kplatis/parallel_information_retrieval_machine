package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that implements the vector for a specific query
 */
public class VectorQueryBuilder {

    private ArrayList<String> queries;
    private String invertedIndexFile;    //keeps the name of the file in which the Inverted Index is stored
    private ArrayList<String> vocabulary;   //keeps the vocabulary of the
    private Double amountOfQueries;
    private Double total_number_of_files;
    public HashMap<Integer,Integer> idkMap;     //keeps the id and the k of each Query

    public VectorQueryBuilder(ArrayList<String> queries, String invertedIndexFile, ArrayList<String> vocabulary, Double amountOfQueries, Double total_number_of_files) {
        idkMap = new HashMap<>();
        this.queries = queries;
        this.invertedIndexFile = invertedIndexFile;
        this.vocabulary = vocabulary;
        this.amountOfQueries = amountOfQueries;
        this.total_number_of_files = total_number_of_files;
    }


    public VectorsBuffer build() throws IOException {

        VectorsBuffer queriesBuffer = new VectorsBuffer(vocabulary,amountOfQueries);    //creates a buffer that contains a vector for each query

        for(String query : queries) {
            HashMap<String,Double> termCount = new HashMap<>();    //contains the <term,count of appearances>

            String[] tokens = query.split(" ");     //breaks the query in tokens
            Integer idOfQuery = Integer.parseInt(tokens[0]);

            idkMap.put(idOfQuery,Integer.parseInt(tokens[1]));
            Double maxf = 1.0;


            for (int i=2;i<tokens.length;i++) {
                String word = tokens[i];
                if (termCount.containsKey(word)) {    //if the word is contained in the term
                    Double count = termCount.get(word);
                    count++;
                    if (count > maxf) {
                        maxf = count;    //changes the max value
                    }
                    termCount.put(word, count);
                } else {
                    termCount.put(word, 1.0);
                }
            }

            for (Map.Entry wordsEntry : termCount.entrySet()) {

                String term = (String) wordsEntry.getKey();
                Double weight = (0.5 *  ( (Double) wordsEntry.getValue() / maxf) + 0.5) * fetchIDFof(term);    //calculates the weight of the specific term
                queriesBuffer.changeWeightValue(new Double(idOfQuery), term, weight);
            }
            idOfQuery++;
        }

        return queriesBuffer;

    }


    private Double fetchIDFof(String term) throws IOException {


        FileInputStream stream = null;
        try {
            stream = new FileInputStream(invertedIndexFile);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));

            String line;

            line = bufferedReader.readLine();

            if (line != null) { //if the file has not ended

                String[] tokens = line.split(" ");  //breaks the line of the line into tokes
                if (tokens[0]==term){
                    Double nt = (tokens.length - 1) / 2.0;  //gets the nt of the term
                    Double idf = Math.log(total_number_of_files / nt);    //calculates the idf of the term
                    return idf;
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return 1.0;
    }

}
