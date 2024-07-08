package org.glowroot.central.util;

import java.io.File;

public interface SPIClusterManager {

    void setup(File confDir) throws Exception;
    ClusterManager create();

}
