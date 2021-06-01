package com.gkatzioura.arrow;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ArrowMain {

    public static void main(String[] args) throws IOException {
        var originalEntries = IntStream.rangeClosed(0, 11)
                             .boxed()
                             .map(i -> new DefaultArrowEntry("data-"+i, i)).collect(Collectors.toList());

        var outputStream = new ByteArrayOutputStream();

        try(var arrowWriter = new DefaultEntriesWriter()) {
            arrowWriter.write(originalEntries, 10, Channels.newChannel(outputStream));
        }

        byte[] introBytes = outputStream.toByteArray();

        var inputStream = new ByteArrayInputStream(introBytes);

        try(var arrowReader = new DefaultEntriesReader()) {
            var entries =arrowReader.readBytes(Channels.newChannel(inputStream));
            for (DefaultArrowEntry entry : entries) {
                System.out.println("Read "+entry.getCol1()+" "+entry.getCol2());
            }
        }

    }

}
