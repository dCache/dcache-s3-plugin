S3 Nearline Storage Plugin for dCache
==================================

This plugin enables dCache to flush, stage and remove
files to and from an S3 endpoint.

Building the plugin
----------------------------

To build this plugin, `git` and `maven` are needed. Please install this
programs first if they're not already present.

Next, the sourcecode has to be downloaded using
`git clone https://github.com/dCache/dcache-s3-plugin.git`. This creates a
folder, called *dcache-hsm-plugin*, where the sourcecode can be found.
To turn this sourcecode into a working plugin, it has to be built with the
help of Maven. `mvn clean package` compiles the code and packs it to a
*\*.tar.gz-file* which is located in *target* subdirectory. This file is
needed for the next step.

Using the plugin with dCache
----------------------------

To use this plugin with dCache, unzip the .tar.gz-file to
*/usr/local/share/dcache/plugins/* on a dCache pool. Restart
the pool to load the plugin.

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