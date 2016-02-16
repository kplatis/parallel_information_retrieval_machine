package com.company;

import java.util.ArrayList;
import java.io.* ;
import java.util.* ;

/**
 * Created by theo on 2/11/16.
 */
public class Merger implements Runnable {

    private Thread t ;
    private String threadName ;
    private String nameOfDocument1 ;
    private String nameOfDocument2 ;

    public Merger(int numThread){
        threadName="Thread "+numThread ;
    }

    public void start(String docName1,String docName2){
        nameOfDocument1=docName1 ;
        nameOfDocument2=docName2 ;
        t=new Thread(this,threadName) ;
        t.start();
    }

    public boolean isRunning(){
        if(t==null)
            return false ;
        return t.isAlive() ;
    }

    /**
     * Method that merge-sorts two files and produces an output file
     */
    public void run() {
        try {

                PrintStream outputStream = new PrintStream(new File(getMergedFileName()));

                BufferedReader firstFileReader = new BufferedReader(new FileReader(new File(nameOfDocument1)));
                BufferedReader secondFileReader = new BufferedReader(new FileReader(new File(nameOfDocument2)));
                String firstLine = firstFileReader.readLine();
                String secondLine = secondFileReader.readLine();
                String firstTerm;
                String secondTerm;

                while (firstLine != null && secondLine != null) {
                    firstTerm = getTermOfLine(firstLine);
                    secondTerm = getTermOfLine(secondLine);

                    if (firstTerm.compareTo(secondTerm) == 0) {   //if the strings are the same
                        String mergedList = mergeLists(firstLine, secondLine);   //merges the lists of (d,f)
                        outputStream.println(mergedList);  //writes the list in the output file

                        firstLine = firstFileReader.readLine();
                        secondLine = secondFileReader.readLine();
                    } else if (firstTerm.compareTo(secondTerm) < 0) {   //firstTerm less than second term
                        outputStream.println(firstLine);
                        firstLine = firstFileReader.readLine();
                    } else {   //secondTerm less than first term

                        outputStream.println(secondLine);
                        secondLine = secondFileReader.readLine();

                    }

                }

                if (firstLine == null && secondLine != null) {     //means that the first file finished first

                    while (secondLine != null) {
                        outputStream.println(secondLine);
                        secondLine = secondFileReader.readLine();
                    }

                }
                if (secondLine == null && firstLine != null) {        //means that the second file finished first

                    while (firstLine != null) {
                        outputStream.println(firstLine);
                        firstLine = firstFileReader.readLine();
                    }
                }
        }
        catch (IOException e){
            e.printStackTrace();
        }

    }

    /**
     * Takes 2 lines from the 2 intermediate files and produces a merge of the 2 to be written in the output file
     * @return the merged line
     */
    private String mergeLists(String firstList,String secondList){

        StringBuilder builder = new StringBuilder(secondList);
        builder.delete(0,builder.indexOf(","));
        return firstList+builder;

    }


    /**
     * Method that isolates the "term" of a line
     * @param line the whole line conatining the "term"
     * @return return the "term"
     */
    private String getTermOfLine(String line){

        String[] tokens = line.split(",");
        return tokens[0];

    }



    /**
     * this method resolves the name of the output file produced by the files merged .The format is :index1.txt+index2.txt=index1_2.txt
     * @return the name of the output file
     */
    private String getMergedFileName(){
        String fileName1=nameOfDocument1 ;
        String fileName2=nameOfDocument2 ;
        fileName2=fileName2.replace("index","");
        fileName2=fileName2.replace(".txt","") ;
        fileName1=fileName1.replace("index","") ;
        fileName1=fileName1.replace(".txt","") ;
        String number1=fileName1.split("_")[0] ;
        String number2 ;
        if(fileName2.contains("_"))
            number2=fileName2.split("_")[1] ;
        else
            number2=fileName2 ;
        return  "index"+number1+"_"+number2+".txt" ;
    }

    public void waitThread(){
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }




}
