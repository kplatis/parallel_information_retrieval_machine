package com.company;

import sun.reflect.generics.tree.DoubleSignature;
import sun.reflect.generics.tree.Tree;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.TreeMap;

public class Main{

    static Double numDocs=new Double(12) ;    //temporary number of Docs MUST SET IT AS PARAMETER
    static final String invertedIndexFileName="InvertedIndex.txt" ;
    static String inputQueriesFile = "input.txt";
    static String resultsFile = "output.txt";

    public static void main(String[] args) throws IOException {
        if(args.length==3) {
            if (args[0] != null) {
                numDocs = new Double(args[0]);
            } //sets the number of files
            if (args[1] != null) {
                inputQueriesFile = args[1];
            } //sets the name of the input file containing the queries
            if (args[2] != null) {
                resultsFile = args[2];
            } //sets the name of the output file for the results
        }



        long start_time = System.currentTimeMillis();

        ThreadExecutor executor=new ThreadExecutor(Runtime.getRuntime().availableProcessors()) ;
        InvertedIndex index=new InvertedIndex() ;
        executor.build(index,numDocs);
        String fileName=executor.merge(new Double(index.getDumpedIndexes())) ;
        executor.renameInvertedIndexFile(fileName,invertedIndexFileName);
        long end_time = System.currentTimeMillis();

        System.out.println("Merge complete. Open "+invertedIndexFileName+" to check the final Inverted Index");

        long difference = end_time-start_time;
        System.out.println("Phase 1 execution completed in "+difference +" milliseconds");


         start_time = System.currentTimeMillis();

        VectorsBuffer documentsBuffer = executor.createDocumentVectors(invertedIndexFileName,12.0);
        VectorsBuffer queriesBuffer = executor.createQueriesVectors(inputQueriesFile,invertedIndexFileName,numDocs);

        HashMap<Double,Double> idkMap = buildIDKMap();
        CosineSimilarity similarity = new CosineSimilarity(idkMap);
        HashMap<Double,ArrayList<Double>> resultsOfSimilarity = similarity.run(documentsBuffer,queriesBuffer);

        IOHandler handler = new IOHandler();
        handler.writeOutputResultsToFile(resultsFile,resultsOfSimilarity);

        end_time = System.currentTimeMillis();

        System.out.println("Output produced in: "+(end_time-start_time)+" milliseconds");

       // System.out.println("Phase 2 execution completed in "+time);

        /*String invertedIndexFile = "index1_192.txt";
        String queriesFile="input.txt";

        VectorsBuffer documentsBuffer =executor.createDocumentVectors(invertedIndexFile,12.0);
        VectorsBuffer queriesBuffer = executor.createQueriesVectors(queriesFile,invertedIndexFile,12.0);

        documentsBuffer.showBuffer();
        //queriesBuffer.showBuffer();
        HashMap<Double,Double> idkMap = new HashMap<>();
        idkMap.put(1.0,3.0);
        idkMap.put(2.0,4.0);
        idkMap.put(3.0,2.0);

        CosineSimilarity sim = new CosineSimilarity(idkMap);
        sim.run(documentsBuffer,queriesBuffer);
        */
    }


    /**
     * Method that creates a map for <id,k> pairs where id=id of the document and k=top-k results
     *
     * @return
     * @throws IOException
     */
    public static HashMap<Double,Double> buildIDKMap() throws IOException {
        HashMap<Double,Double> idkMap = new HashMap<>();
        FileInputStream stream = null;
        try {
            stream = new FileInputStream(inputQueriesFile);
        } catch (FileNotFoundException e) {
            System.out.println("Input File does not exist!");
        }
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));

        String line;
        while((line=bufferedReader.readLine()) != null){
            String[] tokens = line.split(" ");
            idkMap.put(Double.parseDouble( tokens[0] ),Double.parseDouble( tokens[1] ) );
        }
        return idkMap;
    }
}