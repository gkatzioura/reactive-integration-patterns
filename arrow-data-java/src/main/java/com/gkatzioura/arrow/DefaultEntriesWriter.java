package com.gkatzioura.arrow;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.util.Text;

import static com.gkatzioura.arrow.SchemaFactory.DEFAULT_SCHEMA;

public class DefaultEntriesWriter implements Closeable {

    private final RootAllocator rootAllocator;
    private final VectorSchemaRoot vectorSchemaRoot;

    public DefaultEntriesWriter() {
        rootAllocator = new RootAllocator();
        vectorSchemaRoot = VectorSchemaRoot.create(DEFAULT_SCHEMA, rootAllocator);
    }

    public void write(List<DefaultArrowEntry> defaultArrowEntries, int batchSize, WritableByteChannel out) {
        if (batchSize <= 0) {
            batchSize = defaultArrowEntries.size();
        }

        DictionaryProvider.MapDictionaryProvider dictProvider = new DictionaryProvider.MapDictionaryProvider();
        try(ArrowStreamWriter writer = new ArrowStreamWriter(vectorSchemaRoot, dictProvider, out)) {
            writer.start();

            VarCharVector childVector1 = (VarCharVector) vectorSchemaRoot.getVector(0);
            IntVector childVector2 = (IntVector) vectorSchemaRoot.getVector(1);
            childVector1.reset();
            childVector2.reset();

            boolean exactBatches = defaultArrowEntries.size()%batchSize == 0;
            int batchCounter = 0;

            for(int i=0; i < defaultArrowEntries.size(); i++) {
                childVector1.setSafe(batchCounter, new Text(defaultArrowEntries.get(i).getCol1()));
                childVector2.setSafe(batchCounter, defaultArrowEntries.get(i).getCol2());

                batchCounter++;

                if(batchCounter == batchSize) {
                    vectorSchemaRoot.setRowCount(batchSize);
                    writer.writeBatch();
                    batchCounter = 0;
                }
            }

            if(!exactBatches) {
                vectorSchemaRoot.setRowCount(batchCounter);
                writer.writeBatch();
            }

            writer.end();
        } catch (IOException e) {
            throw new ArrowExampleException(e);
        }
    }

    @Override
    public void close() throws IOException {
        vectorSchemaRoot.close();
        rootAllocator.close();
    }

}
