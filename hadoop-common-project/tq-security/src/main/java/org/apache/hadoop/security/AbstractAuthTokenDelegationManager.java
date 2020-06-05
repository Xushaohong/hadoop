package org.apache.hadoop.security;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.authentication.tool.TokenUtil;
import com.tencent.tdw.security.beans.Token;
import com.tencent.tdw.security.beans.TokenIdent;
import com.tencent.tdw.security.beans.TokenInformation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractAuthTokenDelegationManager<TOKEN_ID, TOKEN_INFO> {

  private static final Log LOG = LogFactory
      .getLog(AbstractAuthTokenDelegationManager.class);

  private long maxIdleTime;
  private int maxTokenNum;
  private long tokenRemoverScanInterval;
  private boolean running = false;
  private Thread tokenRemoverThread;
  private static final String FAKE = "THIS_IS_FAKE";
  private static final TokenInformation FAKE_TOKEN_INFO = new TokenInformation(-1, "", "", FAKE);

  public AbstractAuthTokenDelegationManager(long maxIdleTime, int maxTokenCacheNum,
                                            long tokenRemoverScanInterval) {
    this.maxIdleTime = maxIdleTime;
    this.maxTokenNum = maxTokenCacheNum;
    this.tokenRemoverScanInterval = tokenRemoverScanInterval;
  }

  public abstract Converter<TOKEN_ID, TOKEN_INFO> getConverter();

  public long pushToken(TokenIdent tokenIdent, TokenInformation tokenInfo) {
    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't push");
      }
      return -1;
    }

    Preconditions.checkNotNull(tokenIdent);
    Preconditions.checkNotNull(tokenInfo);
    try {
      long id = TokenUtil.push(tokenIdent, tokenInfo);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pushed union token:" + id + "-" + tokenIdent);
      }
      return id;
    } catch (Exception e) {
      LOG.error("push token failed", e);
    }
    return -1;
  }

  public void updateToken(long globalId, TokenInformation tokenInfo) {
    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't update");
      }
      return;
    }

    Preconditions.checkArgument(globalId > -1);
    Preconditions.checkNotNull(tokenInfo);

    try {
      TokenUtil.updateInfo(globalId, tokenInfo);
    } catch (Exception e) {
      LOG.error("push token failed", e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Update token:" + globalId + "-" + tokenInfo);
    }
  }


  public Token pullToken(long globalId) {
    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't pull");
      }
      return null;
    }
    Preconditions.checkArgument(globalId > -1);

    try {
      Token token = TokenUtil.pull(globalId);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pulled token:" + globalId + "-" + token);
      }
      return token;
    } catch (Exception e) {
      LOG.error("Pull globalToken failed", e);
      return null;
    }
  }

  public void cancelToken(long globalId) {
    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't cancel");
      }
      return;
    }

    Preconditions.checkArgument(globalId > -1);

    try {
      TokenUtil.cancel(globalId);
    } catch (Exception e) {
      LOG.error("cancel token failed", e);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Canceled token:" + globalId);
    }
  }

  public TokenInformation fetchTokenInfo(long globalId) {

    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't fetch");
      }
      return null;
    }

    Preconditions.checkArgument(globalId > -1);

    try {
      TokenInformation tInfo = TokenUtil.pullInfo(globalId);
      Preconditions.checkNotNull(tInfo, "remote token is not found,global id=" + globalId);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Fetched token:" + globalId + "-" + tInfo);
      }
      return tInfo;
    } catch (Exception e) {
      LOG.error("fetch token info failed", e);
      return null;
    }
  }

  private TokenInformation addFakeToke(long tokenId, TokenIdent id) {
    TokenInformation fake = FAKE_TOKEN_INFO;
    globalTokens.put(tokenId, new GlobalToken(id, fake));
    return fake;
  }

  private NavigableMap<Long, GlobalToken> globalTokens
      = new ConcurrentSkipListMap<>();

  public TokenInformation getToken(long tokenId, TokenIdent identifier) {

    long now = System.currentTimeMillis();
    GlobalToken token = globalTokens.get(tokenId);
    TokenInformation tokenInfo = null;
    if (token == null) {
      Token remoteToken = pullToken(tokenId);
      if (remoteToken != null) {
        token = new GlobalToken(remoteToken.getTokenId(), remoteToken.getTokenInfo());
        globalTokens.put(tokenId, token);
      }
    }

    if (token != null) {
      if (!token.getTokenId().equals(identifier)) {
        LOG.warn(String.format("Token(%d) identifier is unmatched. %s : %s", tokenId, token.getTokenId(), identifier));
        tokenInfo = FAKE_TOKEN_INFO;
      } else {
        tokenInfo = token.getTokenInfo();
        // token is expired and haven't sync the latest
        if (tokenInfo.getRenewDate() < now && token.getSyncTime() <= tokenInfo
            .getRenewDate()) {
          tokenInfo = fetchTokenInfo(tokenId);
          if (tokenInfo != null) {
            token.updateTokenInfo(tokenInfo);
          }
        }
      }
    }

    if (tokenInfo == null) {
      tokenInfo = addFakeToke(tokenId, identifier);
    }

    return tokenInfo.getIssuer().equals(FAKE) ? null : tokenInfo;
  }

  /**
   * should be called before this object is used
   */
  public void startThreads() {
    Preconditions.checkState(!running);
    synchronized (this) {
      running = true;
      final TokenRemover tokenRemover = new TokenRemover();
      tokenRemoverThread = new Thread(tokenRemover);
      tokenRemoverThread.setDaemon(true);
      tokenRemoverThread.setName(tokenRemover.toString());
      tokenRemoverThread.start();
    }
  }


  /**
   * should be called before this object is used
   */
  public void stopThreads() {
    synchronized (this) {
      running = false;
      if (tokenRemoverThread != null) {
        tokenRemoverThread.interrupt();
        try {
          tokenRemoverThread.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(
              "Unable to join on token removal thread", e);
        }
      }
    }
  }

  /**
   * Remove expired delegation tokens from cache
   */
  private void removeExpiredToken() {
    long now = System.currentTimeMillis();
    Set<TokenIdent> expiredTokens = new HashSet<>();
    Iterator<Entry<Long, GlobalToken>> iter =
        globalTokens.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Long, GlobalToken> entry = iter.next();
      final GlobalToken token = entry.getValue();
      long renewDate = token.getTokenInfo().getRenewDate();
      if (renewDate < now && (now - token.getSyncTime()) > maxIdleTime) {
        expiredTokens.add(token.getTokenId());
        iter.remove();
      }
    }

    for (TokenIdent ident : expiredTokens) {
      LOG.debug("the token is expired or idle:" + ident);
    }
  }

  private class TokenRemover implements Runnable {

    private long lastTokenCacheCleanup;

    @Override
    public void run() {
      LOG.info("Starting expired delegation token remover thread, "
          + "tokenRemoverScanInterval=" + tokenRemoverScanInterval
          / (60 * 1000) + " min(s)");
      try {
        while (running) {
          long now = System.currentTimeMillis();

          if (lastTokenCacheCleanup + tokenRemoverScanInterval < now) {
            removeExpiredToken();
            lastTokenCacheCleanup = now;
          }

          removeExceedToken();
          try {
            Thread.sleep(Math.min(5000, tokenRemoverScanInterval / 2)); // 5 seconds
          } catch (InterruptedException ie) {
            LOG.error("TokenRemover received " + ie);
          }
        }
      } catch (Throwable t) {
        LOG.error("TokenRemover thread received unexpected exception", t);
        Runtime.getRuntime().exit(-1);
      }
    }

    @Override
    public String toString() {
      return "Global token remover";
    }
  }

  private void removeExceedToken() {
    int removeNum = maxTokenNum - globalTokens.size();
    while (removeNum-- > 0) {
      if (globalTokens.pollFirstEntry() == null) {
        break;
      }
    }
  }


  public interface Converter<TOKEN_ID, TOKEN_INFO> {
    TokenIdent toId(TOKEN_ID tId);

    TOKEN_INFO toInfo(TokenInformation tInfo);

    TokenInformation toInfo(TOKEN_INFO tInfo);
  }


  static class GlobalToken extends Token {

    private long syncTime;

    GlobalToken(TokenIdent tokenId,
                TokenInformation tokenInfo) {
      super(tokenId, tokenInfo);
      this.syncTime = System.currentTimeMillis();
    }

    void updateTokenInfo(TokenInformation tokenInfo) {
      setTokenInfo(tokenInfo);
      syncTime = System.currentTimeMillis();
    }

    long getSyncTime() {
      return syncTime;
    }

    @Override
    public long getId() {
      return super.getId();
    }

    @Override
    public TokenIdent getTokenId() {
      return super.getTokenId();
    }

    @Override
    public TokenInformation getTokenInfo() {
      return super.getTokenInfo();
    }
  }

}
