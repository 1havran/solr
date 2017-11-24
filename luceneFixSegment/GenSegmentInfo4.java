package org.apache.lucene.index;

import java.io.IOException;
import java.io.File;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.SimpleFSDirectory;

public class GenSegmentInfo4 {
    /* Automation of the https://stackoverflow.com/questions/35273381/generate-lucene-segments-n-file
    for Lucene 410
    */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            help();
            System.exit(1);
        }

        Codec codec = Codec.getDefault();
        File myPath = new File(args[0]);
        Directory directory = new SimpleFSDirectory(myPath);

        SegmentInfos infos = new SegmentInfos();
        for (int i = 1; i < args.length; i++) {
            infos.add(getSegmentCommitInfo4(codec, directory, args[i]));
        }
        infos.commit(directory);
    }

    private static SegmentCommitInfo getSegmentCommitInfo4(Codec codec, Directory directory, String segmentName) throws IOException {
        SegmentInfo info = codec.segmentInfoFormat().getSegmentInfoReader().read(directory, segmentName, IOContext.READ);
        info.setCodec(codec);
        return new SegmentCommitInfo(info, 1, -1, -1, -1);
    }

    private static void help() {
        System.out.println("Not enough arguments");
        System.out.println("Usage: java -cp lucene-core-4.10.3.jar GenSegmentInfo4 <path to index> [segment1 [segment2 ...] ]");
        System.out.println("Example: java -cp lucene-core-4.10.3.jar GenSegmentInfo4 /home/solr/brokenindex4/data/index _0 _1 _2");
        System.out.println("");
    }
}
