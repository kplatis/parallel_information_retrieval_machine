package com.company;

import java.io.*;
import java.util.ArrayList;
import java.util.logging.FileHandler;

/**
 * Handles the threads and the jobs they have to carry out
 */
public class ThreadExecutor {

    private ArrayList<VectorBuilder> vectorBuilders;
    private ArrayList<Builder> builders =new ArrayList<>() ;
    private ArrayList<Merger> mergers =new ArrayList<>() ;
    private ArrayList<String> vocabulary;
    Double filesRead ;


    /**
     * This method creates the builders that hold the threads to be run
     * @param numCores holds the amount of available cores on the machine
     */
    public ThreadExecutor(int numCores){
        vocabulary = new ArrayList<>();

        vectorBuilders = new ArrayList<>();
        for(int i=0;i<numCores;i++){
            builders.add(new Builder(i)) ;
            mergers.add(new Merger(i));
            vectorBuilders.add(new VectorBuilder(i));
        }
        System.out.println("I just created "+builders.size()+" builders");
        System.out.println("I just created "+mergers.size()+" mergers");
    }

    /**
     * This method creates the builders that read from the files and store the values to a working inverted index .
     * @param index the working inverted index where the values are stored
     * @param numDocs the count of documents the threads are to read from
     */
    public void build(InvertedIndex index,Double numDocs){
      //  System.out.println("There are "+numDocs+" files to read from");
        Double filesRead=new Double(0) ;
      //  System.out.println("checking if all files are read....");
        while(filesRead<numDocs){
           // System.out.println("not all files are read");
            for(Builder builder:builders){
                if(builder.checkIndexBuilt()){
                    InvertedIndex tempIndex=builder.getIndex() ;
                    try
                    {
                        index.addMap(tempIndex.getMap());
                    }
                    catch (OutOfMemoryError e){
                        System.out.println("System run out of memory ,writing index to disk");
                        index.dumpIndex();
                        index.addMap(tempIndex.getMap());
                    }
                }
                if (filesRead.equals(numDocs)){
                    //  all the files are read
                    break ;
                }
                if(!builder.isRunning()){
                    filesRead++ ;
                    builder.start(filesRead);
                }
                if(index.size()>100){
                    index.dumpIndex();
                }
            }
        }
        waitBuilders();
        for(Builder builder:builders){
            if(builder.checkIndexBuilt()){
                index.addMap(builder.getIndex().getMap());
            }
        }
        if(index.size()>0) {
            index.dumpIndex();
        }
    }

    /**
     * This method creates the mergers that read the files to be merged and produce a final intermediate file
     * @param numStartInterDocs this variable holds the count of the initial intermediate files to be merged
     * since more will be created in the process .
     *                          The operation of merging is split into two parts .In the first one all pair of files are merged
     *                          and in the second one any left files from stages where there was an odd number of files are merged
     *                          with the file produced from the first part
     *
     */
    public String merge(Double numStartInterDocs){

        ArrayList<String> filesToMerge =new ArrayList<>() ;
        for(Double i=new Double(1);i<numStartInterDocs+1;i++) {
            filesToMerge.add("index" + i.intValue() + ".txt");
        }
        String finalFile =filesToMerge.get(0); //in case there is only one file
        if(filesToMerge.size()>1) { //this means there are files left to be merged apart from the one added above
            ArrayList<String> newFiles = new ArrayList<>();
            Double mergesToDo = Math.floor(new Double(filesToMerge.size()/2));
            int filesLeftCounter = 0;
            while (mergesToDo>new Double(0)) {
                for (Merger merger : mergers) {
                    if (!merger.isRunning() && mergesToDo >new Double(0)) {
                        String file1 = filesToMerge.get(filesLeftCounter);
                        String file2 = filesToMerge.get(filesLeftCounter + 1);
                        merger.start(file1, file2);
                        mergesToDo--;
                        newFiles.add(resolveOutputFileName(file1, file2)); //the output of files being merged from the "filesLeft" is stored in "newFiles"
                        filesLeftCounter += 2;
                    }
                    if (mergesToDo.equals(new Double(0))) {
                        if(filesToMerge.size()%2 != 0){ //if the the "filesLeft" has an odd number of files itself then the last file
                            newFiles.add(filesToMerge.get(filesLeftCounter)); //gets copied into "newFiles" to be merged with the rest
                        }
                        if (newFiles.size() == 1) { //since the "newFiles" keeps the files produced by a merging ,result of 1 means the final file has been produced
                            finalFile=newFiles.get(0) ;//all the merges have been completed and the final file is in the "newFiles" list
                            break;
                        } else { //if more than one files are in the "newFiles" array then they need to be merged from the start
                            filesToMerge.clear();
                            filesToMerge.addAll(newFiles);
                            mergesToDo =Math.floor(new Double(filesToMerge.size()/2));
                            filesLeftCounter = 0;
                            newFiles.clear();
                        }
                    }
                }

            }
        }
        waitMergers();

        return finalFile ;

    }

    public VectorsBuffer createDocumentVectors(String invertedIndexFile,Double numberOfInitialDocs) throws IOException {
        updateVocabulary(invertedIndexFile);

        VectorsBuffer vectorsBuffer = new VectorsBuffer(vocabulary,numberOfInitialDocs);

        try {
            int total_lines = countLines(invertedIndexFile);

            System.out.println("Reading Inverted Index file that has "+total_lines+" lines-terms");
            Double evenNumbersOfLine=new Double(total_lines/Runtime.getRuntime().availableProcessors()) ;
            Double numOfLinePerThread =Math.floor(evenNumbersOfLine);     //"breaks" the file in to even chunks
            int i=0;
            for(VectorBuilder builder : vectorBuilders){
                System.out.println("Assigning lines "+new Double(i*numOfLinePerThread+1)+" to "+new Double(i*numOfLinePerThread+numOfLinePerThread)+" to "+builder.getThreadName());
                builder.start(numberOfInitialDocs,new Double(i*numOfLinePerThread+1),new Double(i*numOfLinePerThread+numOfLinePerThread),invertedIndexFile,vectorsBuffer);
                i++ ;
            }
            waitVectorBuilders();
            if((evenNumbersOfLine % 1) != 0){
                System.out.println("Reading leftover lines "+i*numOfLinePerThread+1+" to "+total_lines);
                vectorBuilders.get(0).start(numberOfInitialDocs,new Double(i*numOfLinePerThread+1),new Double(total_lines),invertedIndexFile,vectorsBuffer);
            }


        } catch (IOException e) {
            e.printStackTrace();
        }

        waitVectorBuilders();   //waits for the vector builders Threads to finish

        /*for(VectorBuilder builder : vectorBuilders){
            System.out.println("SIZE : "+ builder.getThreadVocabulary().size());
            vocabulary.addAll(builder.getThreadVocabulary());
        }*/


        return vectorsBuffer;

    }

    public VectorsBuffer createQueriesVectors(String inputQueriesFile,String invertedIndexFile,Double numberOfInitialDocs) throws IOException {
        ArrayList<String> queries = getQueriesFromFile(inputQueriesFile);
        VectorQueryBuilder vectorQueryBuilder = new VectorQueryBuilder(queries,invertedIndexFile, vocabulary,new Double(queries.size()),numberOfInitialDocs);
        VectorsBuffer queriesBuffer = vectorQueryBuilder.build();

        return queriesBuffer;
    }

    /**
     * Method that reads the queries from a file and saves them in an Arraylist
     * @param inputFile
     * @return
     */
    private ArrayList<String> getQueriesFromFile(String inputFile){
        ArrayList<String> queries = new ArrayList<>();

        try(BufferedReader br = new BufferedReader(new FileReader(inputFile))) {
            for(String line; (line = br.readLine()) != null; ) {
                queries.add(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return queries;

    }

    private void updateVocabulary(String invertedIndexFile) throws IOException {

        FileInputStream stream = new FileInputStream(invertedIndexFile);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(stream));
        String line;
        while ( (line = bufferedReader.readLine()) !=null){
            vocabulary.add(line.split(",")[0]);
        }
    }

    /**
     * Method that counts the total lines of a file in an optimized way (used for the inverted index file)
     * @param filename
     * @return
     * @throws IOException
     */
    private int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }


    private String resolveOutputFileName(String fileName1,String fileName2){
        fileName2=fileName2.replace("index","");
        fileName2=fileName2.replace(".txt","") ;
        fileName1=fileName1.replace("index","") ;
        fileName1=fileName1.replace(".txt","") ;
        String number1=fileName1.split("_")[0] ;
        String number2= "" ;
        if(fileName2.contains("_"))
            number2=fileName2.split("_")[1] ;
        else
            number2=fileName2 ;
        return  "index"+number1+"_"+number2+".txt" ;
    }

    private void waitBuilders(){
        for(Builder builder:builders){
            if(builder.isRunning())
                builder.waitThread();
        }
        System.out.println("All builders done !");
    }

    private void waitMergers(){
        for(Merger merger:mergers){
            if(merger.isRunning())
                merger.waitThread();
        }
        System.out.println("All mergers done !");

    }

    private void waitVectorBuilders(){
        for(VectorBuilder builder : vectorBuilders){
            builder.waitThread();
        }
    }

    public void renameInvertedIndexFile(String oldName,String newName) {
        File file1=new File(oldName) ;
        File file2=new File(newName) ;
        if(file2.exists()){
            System.out.println("This file name is incorrect (file already exists)");
            return ;
        }
        file1.renameTo(file2) ;

    }

}
