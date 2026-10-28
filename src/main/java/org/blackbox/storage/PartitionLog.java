package org.blackbox.storage;

import org.blackbox.message.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class PartitionLog implements AutoCloseable {

    private final FileChannel channel;
    private long nextOffset = 0;

    // Constructor: accepts the path of the log file.
    public PartitionLog(Path filePath) throws IOException {

        Path parent = filePath.toAbsolutePath().getParent();

        if (parent != null) {
            Files.createDirectories(parent);
        }

        channel = FileChannel.open(
                filePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        recover();
    }

    // Store a message and return its offset.
    public synchronized long append(Message message) throws IOException {

        long offset = nextOffset;

        byte[] keyBytes = message.key() == null
                ? new byte[0]
                : message.key().getBytes(StandardCharsets.UTF_8);

        //done so that files store bin data therefore we convert string to bytes
        byte[] valueBytes =
                message.value().getBytes(StandardCharsets.UTF_8);

        int recordSize = Long.BYTES
                + Long.BYTES
                + Integer.BYTES
                + keyBytes.length
                + Integer.BYTES
                + valueBytes.length;

        ByteBuffer buffer = ByteBuffer.allocate(recordSize);

        buffer.putLong(offset);
        buffer.putLong(message.timestamp());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);
        buffer.flip();//switches the mode from writing to reading to prepare it for writing

        long position = channel.size();

        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);

            if (written <= 0) {
                throw new IOException("Unable to write log record");
            }

            position += written;
        }

        nextOffset++;

        return offset;
    }

    // Read all messages from the file.
    public synchronized List<Message> readAll() throws IOException {

        List<Message> messages = new ArrayList<>();

        long position = 0;
        long size = channel.size();

        while (position < size) {

            ByteBuffer header = ByteBuffer.allocate(
                    Long.BYTES * 2 + Integer.BYTES
            );

            readFully(header, position);
            header.flip();

            long offset = header.getLong();
            long timestamp = header.getLong();
            int keyLength = header.getInt();

            if (keyLength < 0 || keyLength > size - position
                    - Long.BYTES * 2 - Integer.BYTES) {
                throw new IOException("Invalid key length");
            }

            position += Long.BYTES * 2 + Integer.BYTES;

            byte[] keyBytes = new byte[keyLength];

            readFully(ByteBuffer.wrap(keyBytes), position);
            position += keyLength;

            ByteBuffer valueLengthBuffer =
                    ByteBuffer.allocate(Integer.BYTES);

            readFully(valueLengthBuffer, position);
            valueLengthBuffer.flip();

            int valueLength = valueLengthBuffer.getInt();

            if (valueLength < 0) {
                throw new IOException("Invalid value length");
            }

            position += Integer.BYTES;

            if (valueLength > size - position) {
                throw new IOException("Invalid value length");
            }

            byte[] valueBytes = new byte[valueLength];

            readFully(ByteBuffer.wrap(valueBytes), position);
            position += valueLength;

            String key = keyLength == 0
                    ? null
                    : new String(keyBytes, StandardCharsets.UTF_8);

            String value =
                    new String(valueBytes, StandardCharsets.UTF_8);

            if (offset != messages.size()) {
                throw new IOException("Unexpected offset in log");
            }

            messages.add(new Message(key, value, timestamp));
        }

        return messages;
    }

    // Restore the next offset from existing records.
    private void recover() throws IOException {
        nextOffset = readAll().size();
    }

    private void readFully(ByteBuffer buffer, long position)
            throws IOException {

        while (buffer.hasRemaining()) {

            int read = channel.read(buffer, position);

            if (read < 0) {
                throw new IOException("Incomplete log record");
            }

            if (read == 0) {
                throw new IOException("Unable to read log record");
            }

            position += read;
        }
    }

    @Override
    public synchronized void close() throws IOException {
        channel.close();
    }
}