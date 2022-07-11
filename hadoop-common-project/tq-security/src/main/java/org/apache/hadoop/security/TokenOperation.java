package org.apache.hadoop.security;

import com.tencent.tdw.security.beans.TokenIdent;
import com.tencent.tdw.security.beans.TokenInformation;

public class TokenOperation {
    public static enum OperationType {
        PUSH,
        UPDATE,
        CANCEL
    }

    public long getGlobalId() {
        return globalId;
    }

    public TokenInformation getTokenInfo() {
        return tokenInfo;
    }

    public TokenIdent getTokenIdent() {
        return tokenIdent;
    }

    public OperationType getType() {
        return type;
    }



    private final OperationType type;
    private long globalId;
    private TokenInformation tokenInfo;
    private TokenIdent tokenIdent;


    private TokenOperation(OperationType type) {
        this.type = type;
    }

    public TokenOperation(OperationType type, long globalId) {
        this(type);
        this.globalId = globalId;
    }

    public TokenOperation(OperationType type, long globalId, TokenInformation tokenInfo) {
        this(type, globalId);
        this.tokenInfo = tokenInfo;
    }

    public TokenOperation(OperationType type, long globalId, TokenInformation tokenInfo, TokenIdent tokenIdent) {
        this(type, globalId, tokenInfo);
        this.tokenIdent = tokenIdent;
    }

}
