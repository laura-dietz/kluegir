package org.smartcactus.kluegir;

import edu.umass.cs.ciir.cluetator.ClueToken;
import edu.umass.cs.ciir.cluetator.DocumentReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * User: dietz
 * * Date: 3/5/15
 * Time: 12:15 PM
 */
public class Playground {

    public static void main(String[] args) throws IOException {
        System.setProperty("file.encoding", "UTF-8"); // ESSENTIAL!!!!

        String dir = "/home/dietz/kbbridge/code/jjd-ede/queripidia/data/documentdump/query-201/";

        HashMap<String, Integer> docIdLookup = new HashMap<String, Integer>();
        final Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> indexCore = indexFromDirectory(dir, docIdLookup);
        KlugirIndexReader<Integer, String,Integer, String> indexReader = indexReader(indexCore);
        KlugirIndexReader<Integer, String,Integer, String> indexReaderWin4 = indexReaderWindow4(indexCore);

        ArrayList<String> query = new ArrayList<String>();
        query.add("raspberry");
        query.add("pi");
        query.add("JavaScript");



        final HashSet<String> andPolicy = new HashSet<String>();
        andPolicy.add("raspberry");
        andPolicy.add("pi");

        final HashSet<String> orPolicy = new HashSet<String>();
//        orPolicy.add("raspberry");
//        orPolicy.add("pi");


        final HashSet<Integer> documentWhitelist = new HashSet<Integer>();
        documentWhitelist.add(0);
//        documentWhitelist.add(69);
//        documentWhitelist.add(84);

        List<PostingFieldTerm<String, Integer, Integer, String>>  merged = indexReader.getMerged(query, andPolicy, orPolicy, documentWhitelist, 0, 5);

        for(PostingFieldTerm<String, Integer, Integer, String> entry : merged){
            System.out.println("\n\n\n");
            System.out.println("entry.doc = " + entry.doc);
            System.out.print("p.positions = ");
            for(AnnotatedPos<Integer, PostingFieldTerm.TermField<String, String>> p:entry.positions) {
                System.out.print(p.pos + ":"+p.fields+"  ");
            }
            System.out.println();
        }

        System.out.println("============ Now with positions hashed %4 =========");

        List<PostingFieldTerm<String, Integer, Integer, String>>  merged2 = indexReaderWin4.getMerged(query, andPolicy, orPolicy, documentWhitelist, 0, 5);

        for(PostingFieldTerm<String, Integer, Integer, String> entry : merged2){
            System.out.println("\n\n\n");
            System.out.println("entry.doc = " + entry.doc);
            System.out.print("p.positions = ");
            for(AnnotatedPos<Integer, PostingFieldTerm.TermField<String, String>> p:entry.positions) {
                System.out.print(p.pos + ":"+p.fields+"  ");
            }
            System.out.println();
        }




    }

    private static KlugirIndexReader<Integer, String,Integer, String> indexReader(Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> index) {
        Comparator<Integer> intComparator = new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };

        return new KlugirIndexReader<Integer, String, Integer, String>(index, intComparator, intComparator);
    }

    private static KlugirIndexReader<Integer, String,Integer, String> indexReaderWindow4(Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> index) {
        Comparator<Integer> intComparator = new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };

        Comparator<Integer> hashedComparator = new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return new Integer(o1 / 4).compareTo(o2 / 4);
            }
        };


        return new KlugirIndexReader<Integer, String, Integer, String>(index, intComparator, hashedComparator);
    }

    private static Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> indexFromDirectory(String dir, HashMap<String, Integer> docIdLookup) throws IOException {
        File dirFile = new File(dir);

        if (!dirFile.isDirectory() || dirFile.listFiles() == null) {
            throw new RuntimeException("dir " + dir + " is not a directory");
        }

        KluegirStruct<Integer, String, Integer, String> klueg = new KluegirStruct<Integer, String, Integer, String>();


        docIdLookup.put("test", 0);

        klueg.initDocument(0);
        klueg.streamAppend("pi", 0, "text");
        klueg.streamAppend("pi", 1, "text");
        klueg.streamAppend("pi", 2, "text");
        klueg.streamAppend("pi", 3, "text");
        klueg.streamAppend("pi", 4, "text");
        klueg.streamAppend("pi", 5, "text");
        klueg.streamAppend("raspberry", 10, "text");
        klueg.streamAppend("raspberry", 11, "text");
        klueg.streamAppend("raspberry", 12, "text");
        klueg.streamAppend("raspberry", 13, "text");
        klueg.produceDocument();


        File[] listFiles = dirFile.listFiles();
        for (int docId = 1; docId < listFiles.length; docId++) {
            File file = listFiles[docId];
            System.out.println("-- " + file.getName() + " --");

            BufferedReader filereader = new BufferedReader(new FileReader(new File(file.getAbsolutePath())));
            List<ClueToken> tokens = DocumentReader.read(filereader);

            docIdLookup.put(file.getName(), docId);

            klueg.initDocument(docId);
            for (int i = 0; i < tokens.size(); i++) {
                ClueToken tok = tokens.get(i);
                klueg.streamAppend(tok.getNormToken(), i, "text");
                klueg.streamAppend(tok.getCapToken().toLowerCase(), i, "captoken");
                for (String annotation : tok.getEntityWikiTitles())
                    klueg.streamAppend(annotation, i, "EntityWikiTitles");
                for (String annotation : tok.getEntityFreebaseMids())
                    klueg.streamAppend(annotation, i, "EntityFreebaseMids");
                for (String annotation : tok.getFreebaseTypes()) klueg.streamAppend(annotation, i, "FreebaseTypes");
                for (String annotation : tok.getWikiCategories()) klueg.streamAppend(annotation, i, "WikiCategories");
            }
            klueg.produceDocument();

            System.out.println("\n\n\n\n");
        }

        final Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> index = klueg.produceIndexCore();
        return index;

    }


}
