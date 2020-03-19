package org.dcache.test;

import java.util.Map;
import java.util.UUID;

import io.minio.MinioClient;
import io.minio.errors.*;
import org.dcache.pool.nearline.spi.FlushRequest;
import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.RemoveRequest;
import org.dcache.pool.nearline.spi.StageRequest;

public class PluginNearlineStorage implements NearlineStorage
{
    protected final String type;
    protected final String name;

    private final MinioClient minio;

    public PluginNearlineStorage(String type, String name) {
        this.type = type;
        this.name = name;
        try {
            String endpoint = "";
            String accessKey = "";
            String secretKey = "";
            minio = new MinioClient(endpoint, accessKey, secretKey);
        } catch (InvalidEndpointException iee) {
            System.out.println("Exception creating minio client: " + iee);
            throw new RuntimeException("Unable to create Minio client");
        } catch (InvalidPortException ipe) {
            System.out.println("Exception creating minio client: " + ipe);
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
    public void flush(Iterable<FlushRequest> requests)
    {
        System.out.println("Flush");
        for (FlushRequest fRequest : requests
             ) {
            System.out.println("Process file " + fRequest.getReplicaUri());
            try {
                boolean isExist = minio.bucketExists("Plugin-Test");
                if (isExist) {
                    System.out.println("Bucket Plugin-Test exists already");
                } else {
                    System.out.println("Bucket Plugin-Test doesn't exists. Will be created now");
                    minio.makeBucket("Plugin-Test");
                }
            } catch (Exception e) {
                System.out.println("Error orccured: " + e.getClass());
                e.printStackTrace();
            }
//            try {
//                minio.putObject("Test", test, fRequest.getReplicaUri().toString());
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
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
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * Delete all files in {@code requests} from nearline storage.
     *
     * @param requests
     */
    @Override
    public void remove(Iterable<RemoveRequest> requests)
    {
        throw new UnsupportedOperationException("Not implemented");
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

    }
}
