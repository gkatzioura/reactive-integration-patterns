package com.gkatzioura.arrow;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

public class DefaultEntriesReader implements Closeable {

    private final RootAllocator rootAllocator;

    public DefaultEntriesReader() {
        rootAllocator = new RootAllocator(Integer.MAX_VALUE);
    }

    public List<DefaultArrowEntry> readBytes(ReadableByteChannel readableByteChannel) throws IOException {
        List<DefaultArrowEntry> defaultArrowEntries = new ArrayList<>();

        try(ArrowStreamReader arrowStreamReader = new ArrowStreamReader(readableByteChannel, rootAllocator)) {
            var root = arrowStreamReader.getVectorSchemaRoot();

            var childVector1 = (VarCharVector)root.getVector(0);
            var childVector2 = (IntVector)root.getVector(1);

            while (arrowStreamReader.loadNextBatch()) {

                int batchSize = root.getRowCount();

                for (int i = 0; i < batchSize; i++) {
                    var strData = new String(childVector1.get(i));
                    var intData = childVector2.get(i);

                    DefaultArrowEntry defaultArrowEntry = DefaultArrowEntry.builder().col1(strData).col2(intData).build();
                    defaultArrowEntries.add(defaultArrowEntry);
                }
            }

            return defaultArrowEntries;
        }
    }

    @Override
    public void close() throws IOException {
        rootAllocator.close();
    }
}
