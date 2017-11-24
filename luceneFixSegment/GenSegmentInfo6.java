package org.apache.lucene.index;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene62.Lucene62SegmentInfoFormat;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.StringHelper;

public class GenSegmentInfo6 {
    /* Automation of the https://stackoverflow.com/questions/35273381/generate-lucene-segments-n-file
    for Lucene 62
    */
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            help();
            System.exit(1);
        }

        Codec codec = Codec.getDefault();
        Directory directory = new SimpleFSDirectory(Paths.get(args[0]));

        SegmentInfos infos = new SegmentInfos();
        for (int i = 1; i < args.length; i++) {
            infos.add(getSegmentCommitInfo6(codec, directory, args[i]));
        }
        infos.commit(directory);
    }

    private static SegmentCommitInfo getSegmentCommitInfo6(Codec codec, Directory directory, String segmentName) throws IOException {
        byte[] segmentID = new byte[StringHelper.ID_LENGTH];
        final String fileName = IndexFileNames.segmentFileName(segmentName, "", Lucene62SegmentInfoFormat.SI_EXTENSION);
        ChecksumIndexInput input = directory.openChecksumInput(fileName, IOContext.READ);
        DataInput in = new BufferedChecksumIndexInput(input);

        // read headers
        final int actualHeader = in.readInt();
        final String actualCodec = in.readString();
        final int actualVersion = in.readInt();

        in.readBytes(segmentID, 0, segmentID.length);
        SegmentInfo info = codec.segmentInfoFormat().read(directory, segmentName, segmentID, IOContext.READ);

        info.setCodec(codec);
        return new SegmentCommitInfo(info, 1, -1, -1, -1);
    }

    private static void help() {
        System.out.println("Not enough arguments");
        System.out.println("Usage: java -cp lucene-core-6.6.0.jar GenSegmentInfo6 <path to index> [segment1 [segment2 ...] ]");
        System.out.println("Example: java -cp lucene-core-6.6.0.jar GenSegmentInfo6 techproducts_shard1_replica1/data/index _0 _1 _2");
        System.out.println("");
    }
}
