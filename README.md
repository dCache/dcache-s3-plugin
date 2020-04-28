Nearline Storage Plugin for dCache
==================================

This is nearline storage plugin for dCache.

To compile the plugin, run:

    mvn package

This produces a tarball in the `target` directory containing the plugin.

Using the plugin with dCache
----------------------------

To use this plugin with dCache, place the directory containing this
file in /usr/local/share/dcache/plugins/ on a dCache pool. Restart
the pool to load the plugin.

To verify that the plugin is loaded, navigate to the pool in the dCache admin
shell and issue the command:

    hsm show providers

The plugin should be listed.

To activate the plugin, create an HSM instance using:

    hsm create osm name org.dcache.test.test-s3-plugin [-key=value]...

Make sure to provide your endpoint, access- and private-key to the plugin as
the creation will fail otherwise. You can do this by providing the key-value-pairs
to the comment listed above or using the .properties-file in directory defaults.