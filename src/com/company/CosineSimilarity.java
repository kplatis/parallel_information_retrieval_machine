package com.company;

import java.util.*;

/**
 * Class that implements the cosine similarity for a sum of vectors
 */
public class CosineSimilarity {

    private HashMap<Double,Double> idkMap;

    public CosineSimilarity(HashMap<Double,Double> idkMap){
        this.idkMap = idkMap;
    }


    public HashMap<Double,ArrayList<Double>> run(VectorsBuffer documentsBuffer, VectorsBuffer queriesBuffer){
        HashMap<Double,ArrayList<Double>> results = new HashMap<>();

        HashMap<Double,Vector> queriesTable = queriesBuffer.getBuffer();
        HashMap<Double,Vector> documentsTable = documentsBuffer.getBuffer();

        for (Map.Entry queryEntry : queriesTable.entrySet()){
            TreeMap<Double,Double> similarityOfEachPair  = new TreeMap<>();     //keeps the similarity-document pairs
            Vector queryVector = (Vector) queryEntry.getValue();
            ArrayList<Double> topKDocuments = new ArrayList<>();

            for(Map.Entry documentsEntry : documentsTable.entrySet()){
                Vector documentVector = (Vector) documentsEntry.getValue();
                Double similarity = calculateSimilarity(queryVector,documentVector);
                similarityOfEachPair.put(similarity, (Double) documentsEntry.getKey()); //puts the similarity-document pair in a treemap in order to be sorted based in similarity
            }
            TreeMap<Double,Double> descending = new TreeMap<>(similarityOfEachPair.descendingMap());

            Double k = idkMap.get(queryEntry.getKey());    //gets the k of the specific query

            Double counter = 1.0;
            for(Map.Entry descendingEntry : descending.entrySet()){
                topKDocuments.add((Double) descendingEntry.getValue());
                if(k.equals(counter)){
                    break;
                }
                counter++;
            }
            results.put((Double) queryEntry.getKey(),topKDocuments);
        }

        return results;
    }

    private Double calculateSimilarity(Vector vector1,Vector vector2){
        return dotProduct(vector1,vector2) / ( meterOf(vector1) * meterOf(vector2) );
    }



    private Double dotProduct(Vector vector1,Vector vector2){
        Double sum = 0.0;
        for(int i=0;i<vector1.size();i++){
            Double element1 = (Double)vector1.elementAt(i);
            Double element2 = (Double)vector2.elementAt(i);
            sum += element1*element2;
        }
            return sum ;
    }

    private Double meterOf(Vector vector){
        Double sum=0.0;
        for(int i=0;i<vector.size();i++){
            sum+= Math.pow((Double) vector.elementAt(i),2);
        }
        return Math.sqrt(sum);
    }


}
