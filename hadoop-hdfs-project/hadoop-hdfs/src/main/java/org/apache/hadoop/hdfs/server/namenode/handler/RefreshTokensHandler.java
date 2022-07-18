package org.apache.hadoop.hdfs.server.namenode.handler;

import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshResponse;

public class RefreshTokensHandler implements RefreshHandler {

    private final FSNamesystem fsNamesystem;

    public RefreshTokensHandler(FSNamesystem fsNamesystem) {
        this.fsNamesystem = fsNamesystem;
    }

    @Override
    public RefreshResponse handleRefresh(String identifier, String[] args) {
        fsNamesystem.loadTokenFromLocal();
        return RefreshResponse.successResponse();
    }
}
