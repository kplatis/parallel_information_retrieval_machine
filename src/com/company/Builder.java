package com.company;


import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class Builder implements Runnable {

    private InvertedIndex index ; //local index of the thread that is created out of the file that is read
    private Double fileID ;
    private String threadName ;
    private Thread t ;
    private boolean indexBuilt ;


    public Builder(int numThread){
        threadName="Thread "+numThread ;
        index=new InvertedIndex();
        indexBuilt=false ;
    }
    /**
     * contains the work of every thread created
     *
     **/
    public void run(){
        
        HashMap<String,Double> termFrequencyMap = new HashMap<>();  //the map that keeps the term-frequency pairs
        try (BufferedReader br = new BufferedReader(new FileReader(new File(fileID.intValue()+".txt")))) {
            String line;
            StringBuilder builder = new StringBuilder();    //builder that holds the whole document
            while ((line = br.readLine()) != null) {
                line = line.replaceAll(",","");
                builder.append(line);
            }

            String[] tokens = builder.toString().split(" ");     //tokenizes the document into tokens
            for(String token : tokens){
                if (termFrequencyMap.containsKey(token)) {   //if the token already exists in the map
                    Double frequency = termFrequencyMap.get(token) + 1;  //updates term frequency
                    termFrequencyMap.put(token, frequency);
                } else {
                    termFrequencyMap.put(token, new Double(1));  //initialize the term
                }
            }

        } catch (FileNotFoundException e) {
            System.out.println("Im "+threadName+" and could not find file "+fileID+".txt");
        } catch (IOException e) {
            System.out.println("Im "+threadName+" and could not write to file "+fileID+".txt");
        } catch (OutOfMemoryError e){
            System.out.println("Out of memory ,dumping index");
            index.dumpIndex();  //dumps the working inverted index
        }


        for(Map.Entry entry : termFrequencyMap.entrySet()){

            index.insertTermFrequency((String) entry.getKey(), fileID, (Double) entry.getValue());    //for every term-frequency pair - inserts it in the inverted index

        }
        indexBuilt=true ;
    }

    public InvertedIndex getIndex(){
        indexBuilt=false ;
        return index ;
    }

    public boolean checkIndexBuilt(){
        return indexBuilt ;
    }


    public void start(Double afileID){
        indexBuilt=false ;
        index.clearIndex();
        fileID=afileID ;
        t = new Thread(this, threadName);
        t.start();
    }

    public boolean isRunning() {
        if(t==null)
            return false ;
        return t.isAlive();
    }

    public void waitThread(){
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public String getThreadName(){
        return threadName ;
    }

}
