package org.dainst.arachne;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
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
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RecursiveTask;
import org.apache.tika.Tika;

/**
 * Class to crawl directories (multithreaded) and add the file information to a queue.
 *
 * @author Reimar Grabowski
 */
public class DirectoryCrawler extends RecursiveTask<Integer> {

    private final Path root;
    private final BlockingQueue<ArchivedFileInfo> queue;
    private final Tika tika;
    private final int mimeInfo;
    private final boolean strict;

    private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("#.00");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

    private boolean errors = false;
    
    private final boolean verbose;
    
    private int scannedFiles = 0;

    protected DirectoryCrawler(final Path root, final int mimeInfo, final boolean strict, final boolean verbose
            , final BlockingQueue<ArchivedFileInfo> queue) {
        
        this.mimeInfo = mimeInfo;
        this.tika = mimeInfo == 2 ? new Tika() : null;
        this.root = root;
        this.queue = queue;
        this.strict = strict;
        this.verbose = verbose;
    }

    @Override
    protected Integer compute() {
        final List<DirectoryCrawler> crawlers = new ArrayList<>();

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
                    if (!directory.equals(DirectoryCrawler.this.root)) {
                        DirectoryCrawler crawler = new DirectoryCrawler(directory, mimeInfo, strict, verbose, queue);
                        crawler.fork();
                        crawlers.add(crawler);
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        try {
                            scannedFiles++;
                            queue.put(getFileInfo(directory, attrs));
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            errors = true;
                            return FileVisitResult.TERMINATE;
                        }
                        return FileVisitResult.CONTINUE;
                    }
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        scannedFiles++;
                        queue.put(getFileInfo(file, attrs));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        errors = true;
                        return FileVisitResult.TERMINATE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e)
                        throws IOException {

                    if (strict) {
                        System.err.printf("Could not read '%s': ", file);
                        if (e instanceof AccessDeniedException) {
                            System.err.println("Access denied");
                        } else {
                            System.err.println(e.getMessage());
                        }

                        errors = true;
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        System.out.printf("Ignoring '%s': ", file);
                        if (e instanceof AccessDeniedException) {
                            System.out.println("Access denied");
                        } else {
                            System.out.println(e.getMessage());
                        }

                        return FileVisitResult.SKIP_SUBTREE;
                    }
                }
            });
        } catch (IOException e) {
            System.err.println("IO error: " + e.getMessage());
            errors = true;
        }

        crawlers.stream().forEach(crawler -> scannedFiles += crawler.join());
        if (verbose) {
            System.out.println("\r" + root + ": " + scannedFiles + " files");
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

        LocalDateTime creationDateTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(attributes.creationTime().toMillis()), ZoneId.systemDefault());
        LocalDateTime modifiedDateTime =
            LocalDateTime.ofInstant(Instant.ofEpochMilli(attributes.lastModifiedTime().toMillis()), ZoneId.systemDefault());
        
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
