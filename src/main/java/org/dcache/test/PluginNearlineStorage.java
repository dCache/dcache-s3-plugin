package org.dcache.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;
import org.dcache.util.FireAndForgetTask;
import org.xmlpull.v1.XmlPullParserException;

public class PluginNearlineStorage implements NearlineStorage
{
    protected final String type;
    protected final String name;

    private final MinioClient minio;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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
        for (final FlushRequest fRequest : requests
             ) {
            executor.execute(new FireAndForgetTask(new Runnable() {
                @Override
                public void run() {
                    String bucketName = fRequest.getFileAttributes().getStorageClass().toLowerCase()
                            .replaceAll("[^a-z-.]", ".");
                    String pnfsId = fRequest.getFileAttributes().getPnfsId().toString();
                    String source = fRequest.getReplicaUri().getPath();
                    System.out.println("Process " + pnfsId + " in bucket " + bucketName);
//                    System.out.println("Source: " + source);
                    fRequest.activate();

                    try {
                        boolean bucketExists = false;
                        bucketExists = minio.bucketExists(bucketName);
                        if (!bucketExists) {
                            minio.makeBucket(bucketName);
                        }
                    } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException |
                            IOException | InvalidKeyException | NoResponseException | XmlPullParserException |
                            ErrorResponseException | InternalException | InvalidResponseException |
                            RegionConflictException e) {
                        fRequest.failed(e);
                        System.out.println("Creating bucket failed: " + e);
                        return;
                    }

                    try {
                        minio.putObject(bucketName, pnfsId, source, fRequest.getFileAttributes().getSize(),
                                null, null, null);
                    } catch (InvalidKeyException | InsufficientDataException | NoSuchAlgorithmException |
                            NoResponseException | InvalidResponseException | XmlPullParserException |
                            InvalidArgumentException | InvalidBucketNameException | ErrorResponseException |
                            InternalException | IOException e) {
                        fRequest.failed(e);
                        System.out.println("Writing file to S3 tape failed: " + e);
                        return;
                    }
                    try {
                        fRequest.completed(Collections.singleton(new URI(type, name, '/' +
                                fRequest.getFileAttributes().getPnfsId().toString(), null, null)));
                    } catch (URISyntaxException use) {
                        fRequest.failed(use);
                        System.out.println("Marking Request as completed failed, caused by URI: " + use);
                    }
                }
            }));
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
        throw new UnsupportedOperationException("Not implemented");
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
        for (final RemoveRequest rRequest: requests
             ) {
            executor.execute(new Runnable() {
                private Void empty = null;

                @Override
                public void run() {
                    System.out.println("Remove " + rRequest.getUri().getPath().replace("/", ""));
                    rRequest.activate();
                    try {
                        minio.removeObject("test.tape",
                                rRequest.getUri().getPath().replace("/", ""));
                        rRequest.completed(empty);
                    } catch (InvalidKeyException | NoSuchAlgorithmException | NoResponseException |
                            InvalidResponseException | XmlPullParserException | InvalidBucketNameException |
                            InvalidArgumentException | InsufficientDataException | ErrorResponseException |
                            InternalException | IOException e) {
                        rRequest.failed(e);
                        System.out.println("Removing file failed: "+ e);
                    }
                }
            });
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
        System.out.println("Cancel triggered");
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
