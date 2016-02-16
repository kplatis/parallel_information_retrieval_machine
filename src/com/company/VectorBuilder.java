package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Class that implements a Builder responsible to read specific lines from the Inverted Index file
 */
public class VectorBuilder implements Runnable{

    private Double total_number_of_files;
    private int numID;
    private Thread t;
    private String threadName;
    private Double firstLine;   //declares the first line which will be read from the Builder
    private Double lastLine;    //declares the last last to be read from the Builder
    private String filename;    //contains the name of the Inverted Index file
    private VectorsBuffer vectorsBuffer;
    private HashMap<Double,HashMap<String,Double>> results;     //contains the results that will be written in the vector buffer    HashMap<Document,HashMap<Term,W>>   W-> weight

    public VectorBuilder(int i) {
        numID = i;
        threadName = "thread "+numID;
        results = new HashMap<>();
    }

    @Override
    public void run() {

        try {
            FileInputStream stream = new FileInputStream(filename);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));

            String line;
            for(int i=1;i<firstLine;i++){   //pass the first firstLine lines
                bufferedReader.readLine();

            }
            for(int j=0;j<=lastLine-firstLine;j++){   //for the next lastline-firstline lines
                line = bufferedReader.readLine();

                if (line != null) { //if the file has not ended

                    String[] tokens = line.split(",");  //breaks the line of the line into tokes

                    String term = tokens[0];
                    Double nt = (tokens.length - 1) / 2.0;  //gets the nt of the term
                    Double idf = Math.log(total_number_of_files / nt);    //calculates the idf of the term
                    for (int k = 1; k < tokens.length; k += 2) {      //for every (d,f) pair

                        Double document = Double.parseDouble(tokens[k]);


                        Double tf = Double.parseDouble(tokens[k + 1]);
                        Double weight = tf * idf;

                        HashMap<String, Double> termWeightMap;
                        if (results.containsKey(document)) {
                            termWeightMap = results.get(document);   //get the map of the specific document
                        } else {
                            termWeightMap = new HashMap<>();
                        }
                        termWeightMap.put(term, weight);     //puts the new value in the maps
                        results.put(document, termWeightMap);    //puts the map back to the results
                    }

                }

            }
/*
            for(Map.Entry documentEntry : results.entrySet()){      //for every document
                System.out.println("document = " + documentEntry.getKey());

                HashMap<String,Double> termWeightMap = (HashMap<String, Double>) documentEntry.getValue();      //gets the <term,weight> pairs
                for(Map.Entry nestedEntry : termWeightMap.entrySet()){      //for every pair
                    System.out.println("term = "+nestedEntry.getKey());
                    System.out.println("weight = "+nestedEntry.getValue());

                }

            }
*/

            //WRITES IN THE WORKING VECTOR TABLE
            //CHECK BY SEMAPHORE



            for(Map.Entry documentEntry : results.entrySet()){      //for every document
                HashMap<String,Double> termWeightMap = (HashMap<String, Double>) documentEntry.getValue();      //gets the <term,weight> pairs
                for(Map.Entry nestedEntry : termWeightMap.entrySet()){      //for every pair
                    vectorsBuffer.changeWeightValue((Double) documentEntry.getKey(), (String) nestedEntry.getKey(), (Double) nestedEntry.getValue());   //changes the weight value
                }

            }





        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    public void waitThread(){
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void start(Double total_number_of_files,Double firstLine, Double lastLine, String filename , VectorsBuffer vectorsBuffer){
        this.total_number_of_files = total_number_of_files;
        this.firstLine = firstLine;
        this.lastLine = lastLine;
        this.filename = filename;
        this.vectorsBuffer = vectorsBuffer;
        t = new Thread(this,threadName);
        t.run();
    }

    public String getThreadName(){
        return threadName;
    }






}
