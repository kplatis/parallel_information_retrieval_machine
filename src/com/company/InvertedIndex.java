package com.company;


import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class InvertedIndex {


    private TreeMap<String,HashMap<Double,Double>> invertedIndex;   //the inverted index structure
    private int dumpedIndexes;  //keeps the count of indexes that have been already dumped to the disks
    Semaphore semaphore=new Semaphore(1,true) ;

    public InvertedIndex(){
        dumpedIndexes = 0;
        invertedIndex = new TreeMap<>();
    }

    /**
     * Method that inserts a pair (document,frequency) for a specific term
     * If an out of memory error occurs (meaning that RAM is full) return false
     * else return true
     *
     * @param term
     * @param document ID of document
     * @param frequency
     * @return true if the insertion was succesful
     * @return false if the computer is out of memory
     */
    public Boolean insertTermFrequency(String term,Double document,Double frequency){

        try {
            if (invertedIndex.containsKey(term)){   //if the term already exists
                invertedIndex.get(term).put(document,frequency);    //just adds the (document,frequency) pair for the specific term
            }
            else{
                HashMap<Double,Double> newPair = new HashMap<>();   //creates a new HashMap for the new term
                newPair.put(document,frequency);    //inserts the initialization values
                invertedIndex.put(term,newPair);    //put the map in the index
            }
            return true;
        }
        catch (OutOfMemoryError ofme){
            return false;
        }




    }

    public TreeMap<String, HashMap<Double, Double>> getInvertedIndex() {
        return invertedIndex;
    }


    public void setInvertedIndex(TreeMap<String, HashMap<Double, Double>> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }

    public void showIndex(){
        for (Map.Entry entry : invertedIndex.entrySet()){
            System.out.println("=====================   "+entry.getKey()+"  ====================");
            HashMap<Double,Double> nestedMap = (HashMap) entry.getValue();
            for (Map.Entry nestedEntry : nestedMap.entrySet()){
                System.out.println("("+nestedEntry.getKey()+","+nestedEntry.getValue()+")");
            }
        }
    }

    /**
     * Dumps the values of inverted index
     */
    public void dumpIndex(){
        dumpedIndexes++;
        IOHandler handler = new IOHandler();
        handler.writeInvertedIndexInFile(this,"index"+dumpedIndexes+".txt");
        invertedIndex.clear();
    }

    public boolean acquire() throws InterruptedException { //returns true if semaphore acquired or false if not
        try {
            semaphore.acquire();
            return true ;
        }
        catch (InterruptedException e){
            return false ;
        }
    }

    public void release() {
        semaphore.release();
    }

    public int getDumpedIndexes() {
        return dumpedIndexes;
    }

    public int size(){
        return invertedIndex.size() ;
    }

    public void clearIndex(){
        invertedIndex.clear() ;
    }

    public void addMap(TreeMap<String,HashMap<Double,Double>> map){
        for(Map.Entry entry: map.entrySet()){
            if(invertedIndex.containsKey(entry.getKey())){
                invertedIndex.get(entry.getKey()).putAll(map.get(entry.getKey())) ;
            }
            else{
                invertedIndex.put((String)entry.getKey(),(HashMap) entry.getValue()) ;
            }
        }
    }

    public TreeMap<String,HashMap<Double,Double>> getMap(){
        return invertedIndex ;
    }
}
