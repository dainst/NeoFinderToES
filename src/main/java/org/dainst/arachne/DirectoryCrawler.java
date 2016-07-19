package org.dainst.arachne;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import org.apache.tika.Tika;

/**
 * Class to crawl directories and add the file information to a queue.
 *
 * @author Reimar Grabowski
 */
public class DirectoryCrawler implements Callable<Integer> {

    private final Path root;
    private final BlockingQueue<ArchivedFileInfo> queue;
    private final Tika tika;
    private final int mimeInfo;

    private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("#.00");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

    private final boolean verbose;

    private int scannedFiles = 0;

    private final List<String> failedFiles = new ArrayList<>();

    protected DirectoryCrawler(final Path root, final int mimeInfo, final boolean verbose, final BlockingQueue<ArchivedFileInfo> queue) {

        this.mimeInfo = mimeInfo;
        this.tika = mimeInfo == 2 ? new Tika() : null;
        this.root = root;
        this.queue = queue;
        this.verbose = verbose;
    }

    @Override
    public Integer call() {
        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
                    try {
                        scannedFiles++;
                        queue.put(getFileInfo(directory, attrs));
                        if (verbose) {
                            System.out.println("\rScanning " + directory + "...");
                        }
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        failedFiles.add(directory.toString());
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        scannedFiles++;
                        queue.put(getFileInfo(file, attrs));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        failedFiles.add(file.toString());
                        return FileVisitResult.CONTINUE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e) throws IOException {
                    System.err.printf("Could not read '%s': ", file);
                    if (e instanceof AccessDeniedException) {
                        System.out.println("Access denied");
                    } else {
                        System.out.println(e.getMessage());
                    }
                    failedFiles.add(file.toString());
                    return FileVisitResult.CONTINUE;
                }

            });
        } catch (IOException e) {
            System.err.println("IO error: " + e.getMessage());
            failedFiles.add(root.toString());
        }

        if (!failedFiles.isEmpty()) {
            System.err.println("Could not import file information for: ");
            failedFiles.stream().forEach(file -> System.err.println("- " + file));
        }

        return scannedFiles;
    }

    private ArchivedFileInfo getFileInfo(final Path path, final BasicFileAttributes attributes) throws IOException {
        String type = null;
        if (path.toFile().isDirectory()) {
            type = "folder";
        } else {
            switch (mimeInfo) {
                case 1: {
                    type = Files.probeContentType(path);
                    break;
                }

                case 2: {
                    type = tika.detect(path);
                    break;
                }

                default:
                    type = type != null ? type : "n/a";
            }

        }
        Path fileName = path.getFileName();
        fileName = fileName != null ? fileName : path;

        LocalDateTime creationDateTime
                = LocalDateTime.ofInstant(Instant.ofEpochMilli(attributes.creationTime().toMillis()), ZoneId.systemDefault());
        LocalDateTime modifiedDateTime
                = LocalDateTime.ofInstant(Instant.ofEpochMilli(attributes.lastModifiedTime().toMillis()), ZoneId.systemDefault());

        return new ArchivedFileInfo(null, false)
                .setName(fileName.toString())
                .setPath(path.toString())
                .setSize(formatSize(attributes.size()))
                .setCreated(creationDateTime.format(DATE_FORMATTER))
                .setLastChanged(modifiedDateTime.format(DATE_FORMATTER))
                .setResourceType(type);
    }

    private String formatSize(final long sizeInBytes) {
        if (sizeInBytes < 1024) {
            return sizeInBytes + " B" + " (" + NumberFormat.getNumberInstance(Locale.GERMAN).format(sizeInBytes) + " Bytes)";
        }
        int z = (63 - Long.numberOfLeadingZeros(sizeInBytes)) / 10;
        return DECIMAL_FORMATTER.format((double) sizeInBytes / (1L << (z * 10))) + " " + " KMGTPE".charAt(z) + "B ("
                + NumberFormat.getNumberInstance(Locale.US).format(sizeInBytes) + " Bytes)";
    }
}
