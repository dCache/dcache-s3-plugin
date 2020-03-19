package org.dcache.test;

import java.util.Properties;

import org.dcache.pool.nearline.spi.NearlineStorage;
import org.dcache.pool.nearline.spi.NearlineStorageProvider;

public class PluginNearlineStorageProvider implements NearlineStorageProvider
{
    @Override
    public String getName()
    {
        return "org.dcache.test.test-s3-plugin";
    }

    @Override
    public String getDescription()
    {
        return "A test plugin for use with S3";
    }

    @Override
    public NearlineStorage createNearlineStorage(String type, String name)
    {
        return new PluginNearlineStorage(type, name);
    }
}
