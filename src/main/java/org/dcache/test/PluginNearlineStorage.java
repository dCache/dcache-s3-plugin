package org.dcache.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.List;
import java.util.concurrent.*;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.xmlpull.v1.XmlPullParserException;

public class PluginNearlineStorage implements NearlineStorage
{
    protected final String type;
    protected final String name;

    private final MinioClient minio;
    private final ExecutorService executor = Executors.newFixedThreadPool(3);
    private List<FutureTask> taskList = new ArrayList<FutureTask>();

    public PluginNearlineStorage(String type, String name) {
        this.type = type;
        this.name = name;
        try {
            String endpoint = "";
            String accessKey = "";
            String secretKey = "";
            minio = new MinioClient(endpoint, accessKey, secretKey);
        } catch (InvalidEndpointException | InvalidPortException iee) {
            System.out.println("Exception creating minio client: " + iee);
            throw new RuntimeException("Unable to create Minio client");
        } catch (Exception e) {
            System.out.println("Unknown error: " + e);
            throw new RuntimeException("Unable to create Minio client");
        }
    }

    /**
     * Flush all files in {@code requests} to nearline storage.
     *
     * @param requests
     */
    @Override
    public void flush(final Iterable<FlushRequest> requests)
    {
        System.out.println("Flush triggered");

        for (FlushRequest fRequest: requests
             ) {
            FutureTask<UUID> flushTask = new FutureTask<UUID>(new Callable() {
                @Override
                public UUID call() throws Exception {
                    System.out.println("Flush file " + fRequest.getReplicaUri().getPath() + " -- " + fRequest.getId());

                    String bucketName = fRequest.getFileAttributes().getStorageClass().toLowerCase()
                            .replaceAll("[^a-z-.]", ".");
                    String pnfsId = fRequest.getFileAttributes().getPnfsId().toString();
                    String source = fRequest.getReplicaUri().getPath();
                    fRequest.activate();
                    try {
                        boolean bucketExists = minio.bucketExists(bucketName);
                        if (!bucketExists) {
                            minio.makeBucket(bucketName);
                        }
                    } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException |
                            IOException | InvalidKeyException | NoResponseException | XmlPullParserException |
                            ErrorResponseException | InternalException | InvalidResponseException |
                            RegionConflictException e) {
                        fRequest.failed(e);
                        System.out.println("Flush " + pnfsId + " failed, error with bucket exists and creation: " + e);
                        return null;
                    }
                    try {
                        minio.putObject(bucketName, pnfsId, source, fRequest.getFileAttributes().getSize(),
                                null, null, null);
                    } catch (InvalidKeyException | InsufficientDataException | NoSuchAlgorithmException |
                            NoResponseException | InvalidResponseException | XmlPullParserException |
                            InvalidArgumentException | InvalidBucketNameException | ErrorResponseException |
                            InternalException | IOException e) {
                        fRequest.failed(e);
                        System.out.println("Writing file " + pnfsId + " to S3 tape failed: " + e);
                        return null;
                    }
                    try {
                        fRequest.completed(Collections.singleton(new URI(type, name, '/' +
                                fRequest.getFileAttributes().getPnfsId().toString(), null, null)));
                    } catch (URISyntaxException use) {
                        fRequest.failed(use);
                        System.out.println("Marking Request as completed failed, caused by URI: " + use);
                    }
                    System.out.println("File " + source + " flushed to tape with pnfsID " + pnfsId);
                    return fRequest.getId();
                }
            });
            taskList.add(flushTask);
            executor.execute(flushTask);
        }

    }

    /**
     * Stage all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void stage(Iterable<StageRequest> requests)
    {
        System.out.println("Stage triggered");

        for (final StageRequest sRequest : requests
             ) {
            executor.execute(() -> {
                sRequest.activate();
                sRequest.allocate();
                String bucketName = sRequest.getFileAttributes().getStorageClass().toLowerCase()
                        .replaceAll("[^a-z-.]", ".");
                String objectName = sRequest.getFileAttributes().getPnfsId().toString();
                try {

                    minio.getObject(bucketName, objectName, "test123.txt");
                    sRequest.completed(sRequest.getFileAttributes().getChecksumsIfPresent().isPresent() ?
                            sRequest.getFileAttributes().getChecksums() : null);
                } catch (InvalidKeyException | NoSuchAlgorithmException | NoResponseException |
                        InvalidResponseException | XmlPullParserException | InvalidBucketNameException |
                        InvalidArgumentException | InsufficientDataException | ErrorResponseException |
                        InternalException | IOException e) {
                    sRequest.failed(e);
                    System.out.println("Stage request failed: " + e);
                }
            });
        }
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(final Iterable<RemoveRequest> requests)
    {
        System.out.println("Remove triggered");

        for (RemoveRequest rRequest: requests
             ) {

            FutureTask ft = new FutureTask(new Callable() {
                @Override
                public Object call() throws Exception {
                    System.out.println("Remove " + rRequest.getUri().getPath().replace("/", ""));
                    rRequest.activate();
                    try {
                        minio.removeObject("test.tape",
                                rRequest.getUri().getPath().replace("/", ""));
                        rRequest.completed(null);
                        return rRequest.getId();
                    } catch (InvalidKeyException | NoSuchAlgorithmException | NoResponseException |
                            InvalidResponseException | XmlPullParserException | InvalidBucketNameException |
                            InvalidArgumentException | InsufficientDataException | ErrorResponseException |
                            InternalException | IOException e) {
                        rRequest.failed(e);
                        System.out.println("Removing file failed: "+ e);
                        return null;
                    }
                }
            });
            executor.execute(ft);
            taskList.add(ft);
        }
    }

    /**
     * Cancel any flush, stage or remove request with the given id.
     * <p>
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the cancellation should be ignored and the completed
     * or failed method should be called as appropriate.
     * <p>
     * A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    @Override
    public void cancel(UUID uuid)
    {
        System.out.println("Cancel triggered for " + uuid.toString());
        for (FutureTask ft: taskList
             ) {
            String fileId = null;
            try {
                fileId = ft.get().toString();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            if (uuid.toString().equals(fileId)) {
                System.out.println("Cancel Task with uuid " + uuid);
                ft.cancel(true);
            }
        }
    }

    /**
     * Applies a new configuration.
     *
     * @param properties
     * @throws IllegalArgumentException if the configuration is invalid
     */
    @Override
    public void configure(Map<String, String> properties) throws IllegalArgumentException
    {
        System.out.println("Configure triggered");
    }

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     * <p>
     * This method does not wait for actively executing requests to
     * terminate.
     */
    @Override
    public void shutdown()
    {
        System.out.println("Shutdown triggered");
    }
}
