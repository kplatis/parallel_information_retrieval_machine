package com.company;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.Semaphore;

public class VectorsBuffer {

    private ArrayList<String> vocabulary;
    private HashMap<Double,Vector> buffer;  //contains the buffer of the vectors    Double -> ID of Document
    private Double amountOfDocs;
    private Semaphore semaphore;


    public VectorsBuffer(ArrayList<String> vocabulary,Double amountOfDocs){
        semaphore = new Semaphore(1,true);
        buffer = new HashMap<>();
        this.vocabulary = vocabulary;
        this.amountOfDocs = amountOfDocs;
        initializeVectors();
    }

    /**
     * Method that acquires the semaphore
     * @return
     */
    public Boolean acquire(){
        try {
            semaphore.acquire();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * Method that releases the semaphore
     */
    public void release(){
        semaphore.release();
    }


    public HashMap<Double, Vector> getBuffer() {
        return buffer;
    }

    public ArrayList<String> getVocabulary() {
        return vocabulary;
    }

    public void insertWordToVocabulary(String word){
        vocabulary.add(word);
    }

    private void initializeVectors(){

        for(Double i=1.0;i<=amountOfDocs;i++){    //for every doc

            ArrayList<Double> list = new ArrayList<>();
            for (String word : vocabulary){ //initialization of vector
                list.add(0.0);
            }

            Vector vector = new Vector(list);   //creates a vector containing Vocavulary size Zeros
            buffer.put(i,vector);
        }

    }

    /**
     * Changes the weight value of the term in the specific document
     *
     * @param document
     * @param term
     * @param weight
     */
    public void changeWeightValue(Double document,String term,Double weight){

        int index = getIndexOf(term);
        Vector docVector = buffer.get(document);
        docVector.setElementAt(weight,index);

    }

    /**
     * Method that returns the index of a specific word in the ArrayList
     * @param word
     * @return
     */
    public int getIndexOf(String word){
        if(vocabulary.contains(word)){
            return vocabulary.indexOf(word);
        }
        else{
            return 1;
        }
    }

    public void showBuffer(){
        for(Map.Entry entry : buffer.entrySet()){
            System.out.println(entry.getKey()+"     ====>   "+entry.getValue());
        }
    }

}
