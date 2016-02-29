package org.dainst.arachne;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RecursiveAction;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.tika.Tika;

/**
 * Class to crawl directories (multithreaded) and add the file information to a queue.
 *
 * @author Reimar Grabowski
 */
public class DirectoryCrawler extends RecursiveAction {

    private final Path root;
    private final BlockingQueue<ArchivedFileInfo> queue;
    private final Tika tika;
    private final int mimeInfo;
    private static final DecimalFormat DECIMAL_FORMATTER = new DecimalFormat("#.00");
    private static final FastDateFormat DATE_FORMATTER = FastDateFormat.getInstance("MM/dd/yyyy HH:mm:ss");

    protected DirectoryCrawler(final Path root, final int mimeInfo, final BlockingQueue<ArchivedFileInfo> queue) {
        this.mimeInfo = mimeInfo;
        this.tika = mimeInfo == 2 ? new Tika() : null;
        this.root = root;
        this.queue = queue;
    }

    @Override
    protected void compute() {
        final List<DirectoryCrawler> crawlers = new ArrayList<>();

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path directory, BasicFileAttributes attrs) throws IOException {
                    if (!directory.equals(DirectoryCrawler.this.root)) {
                        DirectoryCrawler crawler = new DirectoryCrawler(directory, mimeInfo, queue);
                        crawler.fork();
                        crawlers.add(crawler);
                        return FileVisitResult.SKIP_SUBTREE;
                    } else {
                        try {
                            queue.put(getFileInfo(directory, attrs));
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            Logger.getLogger(DirectoryCrawler.class.getName()).log(Level.SEVERE, null, ex);
                        }
                        return FileVisitResult.CONTINUE;
                    }
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    try {
                        queue.put(getFileInfo(file, attrs));
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                        Logger.getLogger(DirectoryCrawler.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException e)
                        throws IOException {
                    System.err.printf("Ignoring %s: ", file);
                    if (e instanceof AccessDeniedException) {
                        System.err.println("Access denied");
                    } else {
                        System.err.println(e.getMessage());
                    }

                    return FileVisitResult.SKIP_SUBTREE;
                }
            });
        } catch (IOException e) {
            if (e instanceof FileSystemException) {
                System.err.println("Ignoring " + e.getMessage());
            } else {
                e.printStackTrace();
            }
        }

        crawlers.stream().forEach((crawler) -> crawler.join());
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
        return new ArchivedFileInfo()
                .setName(fileName.toString())
                .setPath(path.toString())
                .setSize(formatSize(attributes.size()))
                .setCreated(DATE_FORMATTER.format(new Date(attributes.creationTime().toMillis())))
                .setLastChanged(DATE_FORMATTER.format(new Date(attributes.lastModifiedTime().toMillis())))
                .setResourceType(type);
    }

    public String formatSize(final long sizeInBytes) {
        if (sizeInBytes < 1024) {
            return sizeInBytes + " B" + " (" + NumberFormat.getNumberInstance(Locale.GERMAN).format(sizeInBytes) + " Bytes)";
        }
        int z = (63 - Long.numberOfLeadingZeros(sizeInBytes)) / 10;
        return DECIMAL_FORMATTER.format((double) sizeInBytes / (1L << (z * 10))) + " " + " KMGTPE".charAt(z) + "B ("
                + NumberFormat.getNumberInstance(Locale.US).format(sizeInBytes) + " Bytes)";
    }
}
