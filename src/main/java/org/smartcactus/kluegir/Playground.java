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
        KluegirReadableIndex<Integer, String,Integer, String> index = traverseDirectory(dir, docIdLookup);

        ArrayList<String> query = new ArrayList<String>();
        query.add("raspberry");
        query.add("pi");
        query.add("JavaScript");
//        List<TermPosting<String, Integer, Integer, String>> termPostings = index.get(query);
//        for(TermPosting<String, Integer, Integer, String> entry : termPostings){
//            System.out.println("\n\n\n");
//            System.out.println("entry._1 = " + entry._1);
//            for(Posting<Integer, Integer, String> p:entry.posting) {
//                System.out.println("p.doc = " + p.doc);
//                System.out.print("p.positions = ");
//                for(AnnotatedPos<Integer, String> pos:p.positions) {
////                for(KluegirStruct.AnnotatedPos<Integer, String> pos:p.positions) {
//                    System.out.print(pos.pos + ", ");
//                }
//                System.out.println();
//            }
//        }


        final HashSet<String> andPolicy = new HashSet<String>();
        andPolicy.add("raspberry");
        andPolicy.add("pi");

        final HashSet<String> orPolicy = new HashSet<String>();
//        orPolicy.add("raspberry");
//        orPolicy.add("pi");


        final HashSet<Integer> documentWhitelist = new HashSet<Integer>();
        documentWhitelist.add(68);
        documentWhitelist.add(83);

        List<PostingFieldTerm<String, Integer, Integer, String>>  merged = index.getMerged(query, andPolicy, orPolicy, documentWhitelist);



        for(PostingFieldTerm<String, Integer, Integer, String> entry : merged){
            System.out.println("\n\n\n");
            System.out.println("entry.doc = " + entry.doc);
            System.out.print("p.positions = ");
            for(AnnotatedPos<Integer, PostingFieldTerm.TermField<String, String>> p:entry.positions) {
                System.out.print(p.pos + ":"+p.fields+"  ");
//                for(PostingFieldTerm.TermField<String, String> termField:p.fields) {
//                }
            }
            System.out.println();
        }




    }

    private static KluegirReadableIndex traverseDirectory(String dir, HashMap<String,Integer> docIdLookup) throws IOException {
        File dirFile = new File(dir);

        if (!dirFile.isDirectory() || dirFile.listFiles() == null) {
            throw new RuntimeException("dir " + dir + " is not a directory");
        }

        KluegirStruct<Integer, String, Integer, String> klueg = new KluegirStruct<Integer, String, Integer, String>();

        File[] listFiles = dirFile.listFiles();
        for (int docId = 0; docId < listFiles.length; docId++) {
            File file = listFiles[docId];
            System.out.println("-- " + file.getName() + " --");

            BufferedReader filereader = new BufferedReader(new FileReader(new File(file.getAbsolutePath())));
            List<ClueToken> tokens = DocumentReader.read(filereader);

            docIdLookup.put(file.getName(), docId);

            klueg.initDocument(docId);
            for (int i = 0; i < tokens.size(); i++) {
                ClueToken tok = tokens.get(i);
                klueg.streamAppend(tok.getNormToken(), i, "text");
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

        Comparator<Integer> intComparator = new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        };
        final Map<String, ArrayList<PostingFieldTerm<String, Integer, Integer, String>>> index = klueg.produceIndexCore();
        return new KluegirReadableIndex<Integer, String, Integer, String>(index, intComparator, intComparator);

    }


}
