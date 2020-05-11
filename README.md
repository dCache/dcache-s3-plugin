S3 Nearline Storage Plugin for dCache
==================================

This plugin enables dCache to flush, stage and remove
files to and from an S3-endpoint.

Using the plugin with dCache
----------------------------

To use this plugin with dCache, unzip the .tar.gz-file to */usr/local/share/dcache/plugins/*
on a dCache pool. Restart the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed as `org.dcache.nearline-s3`

To activate the plugin, create an HSM instance and pass the endpoint, access- and
secret-key with it. There are two ways to do this:

Pass the information as key-value-pair while creating the plugin, like this:

    hsm create osm <name> org.dcache.nearline-s3 -endpoint=<endpoint-url> -access_key=<access-key> -secret_key=<secret-key>

Or create a .properties-file, which contains the information, and pass the location
to the HSM instance:

    hsm create osm <name> org.dcache.nearline-s3 -conf_file=<path-to-.properties-file>

The .properties-file has to contain the following information:

    endpoint=<endpoint-url>
    access_key=<access-key>
    secret_key=<secret-key>