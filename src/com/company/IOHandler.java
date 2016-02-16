package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class IOHandler {

    /**
     *
     * @param invertedIndex the II to be written
     * @param filename the name of the file
     * @return true if the writing was successful
     * @return false if the writing failed
     */
    public Boolean writeInvertedIndexInFile(InvertedIndex invertedIndex,String filename){


        try {
            PrintStream fileStream = new PrintStream(new File(filename));

            for(Map.Entry entry : invertedIndex.getInvertedIndex().entrySet()){
                StringBuilder builder = new StringBuilder();    //create a String builder in order to create a single line for the file
                builder.append(entry.getKey()); //appends the term inside the builder
                HashMap<Double,Double> nestedMap = (HashMap) entry.getValue();
                for (Map.Entry nestedEntry : nestedMap.entrySet()){     //for every (Doc,Frequency) Pair
                    builder.append(","+nestedEntry.getKey()+","+nestedEntry.getValue());    //appends document and frequency in builder
                }
                fileStream.println(builder);
            }
            fileStream.close();
            return true;    //if the printing was successful return true
        } catch (FileNotFoundException e) {
            System.out.println("Writing index to file failed for file"+filename);
            return false;
        }

    }

    public void writeOutputResultsToFile(String filename, HashMap<Double,ArrayList<Double>> results){

        try{
            PrintStream fileStream = new PrintStream(new File(filename));


            for(Map.Entry outerEntries : results.entrySet()){
                StringBuilder builder = new StringBuilder();
                builder.append("doc: "+outerEntries.getKey()+"   ");
                int counter=1;
                for(Double document : (ArrayList<Double>) outerEntries.getValue()){
                    builder.append(counter+") "+document+"  ");
                    counter++;
                }
                fileStream.println(builder);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

}
