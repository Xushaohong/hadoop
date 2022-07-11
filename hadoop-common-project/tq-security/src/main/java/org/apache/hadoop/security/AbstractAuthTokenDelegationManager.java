package org.apache.hadoop.security;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.authentication.tool.TokenUtil;
import com.tencent.tdw.security.beans.Token;
import com.tencent.tdw.security.beans.TokenIdent;
import com.tencent.tdw.security.beans.TokenInformation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.TokenOperation.OperationType;

public abstract class AbstractAuthTokenDelegationManager<TOKEN_ID, TOKEN_INFO> {

  private static final Log LOG = LogFactory
      .getLog(AbstractAuthTokenDelegationManager.class);

  private static final int MAX_RETRY_TIME = 3;

  private long maxIdleTime;
  private int maxTokenNum;
  private boolean running = false;
  private Thread tokenAsyncThread;
  private final BlockingQueue<TokenOperation> tokenQueue;
  private final AtomicLong currentGettingRequests = new AtomicLong(0);
  private NavigableMap<Long, GlobalToken> globalTokens
          = new ConcurrentSkipListMap<>();
  private final Snowflake snowflake;
  private static final String FAKE = "THIS_IS_FAKE";
  private static final TokenInformation FAKE_TOKEN_INFO = new TokenInformation(-1, "", "", FAKE);

  public AbstractAuthTokenDelegationManager(long maxIdleTime, int maxTokenCacheNum,
          String serviceId) {
    this.maxIdleTime = maxIdleTime;
    this.maxTokenNum = maxTokenCacheNum;
    this.tokenQueue = new LinkedBlockingQueue<>(AuthConfigureHolder.getUnionTokenAsyncCapacity());
    int workerId = convertToWorkerId(serviceId);
    this.snowflake = new Snowflake(workerId);

  }

  private int convertToWorkerId(String serviceId) {
    try {
      int workerId = TokenUtil.getWorkerId(serviceId);
      LOG.info("Got worker id " + workerId + " for " + serviceId);
      return workerId;
    } catch (Exception ex) {
      LOG.error("Cannot get workerId from remote service, fallback using worker id "
              + "from local config, id is " + AuthConfigureHolder.getUnionTokenWorkerId(), ex);
      return AuthConfigureHolder.getUnionTokenWorkerId();
    }
  }

  public int getGlobalTokenNum() {
    return globalTokens.size();
  }

  public int getPendingTokenNum() {
   return tokenQueue.size();
  }

  public abstract Converter<TOKEN_ID, TOKEN_INFO> getConverter();

  public long getNextId() {
    return snowflake.nextId();
  }

  public boolean pushToken(long globalId, TokenIdent tokenIdent, TokenInformation tokenInfo) {
    if (!AuthConfigureHolder.isUnionTokenEnable()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("AuthConfigureHolder is disable, won't push");
      }
      return false;
    }

    Preconditions.checkNotNull(tokenIdent);
    Preconditions.checkNotNull(tokenInfo);
    try {
      boolean isSuccess = tokenQueue.offer(
              new TokenOperation(OperationType.PUSH, globalId, tokenInfo, tokenIdent),
              AuthConfigureHolder.getUnionTokenAsyncInterval(), TimeUnit.MILLISECONDS);
      if (!isSuccess) {
        LOG.debug("Push union token for " + tokenIdent + " not success");
        return false;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Pushed union token:" + globalId + "-" + tokenIdent);
      }
      return true;
    } catch (Exception e) {
      LOG.error("push token failed", e);
    }
    return false;
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
      boolean isSuccess = tokenQueue.offer(
              new TokenOperation(OperationType.UPDATE, globalId, tokenInfo),
              AuthConfigureHolder.getUnionTokenAsyncInterval(), TimeUnit.MILLISECONDS);
      if (!isSuccess) {
        LOG.debug("Update union token for " + globalId + " not success");
      }
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

    Token token = null;
    int count = 0;
    while (token == null && count < MAX_RETRY_TIME) {
      try {
        token = TokenUtil.pull(globalId);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Pulled token:" + globalId + "-" + token);
        }
        return token;
      } catch (Exception e) {
        LOG.error("Pull globalToken failed", e);
      }
      try {
        LOG.warn("Cannot get token from remote, retry getting again for " + globalId);
        TimeUnit.MILLISECONDS.sleep(AuthConfigureHolder.getUnionTokenAsyncInterval());
      } catch (Exception ignored) {}
      count += 1;
    }
    return null;
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
      boolean isSuccess = tokenQueue.offer(
              new TokenOperation(OperationType.CANCEL, globalId),
              AuthConfigureHolder.getUnionTokenAsyncInterval(), TimeUnit.MILLISECONDS);
      if (!isSuccess) {
        LOG.debug("Cancel union token for " + globalId + " not success");
      } else {
        // cancel global cache
        globalTokens.remove(globalId);
      }
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

  public TokenInformation getToken(long tokenId, TokenIdent identifier) {

    long now = System.currentTimeMillis();
    GlobalToken token = globalTokens.get(tokenId);
    TokenInformation tokenInfo = null;
    // if token is null, that means token is not cached, try to request from center db.
    if (token == null) {
      // since this is a heavy rpc, we should limit the max thread of getting token from global
      if (currentGettingRequests.incrementAndGet() > AuthConfigureHolder.getUnionTokenMaxGettingRequest()) {
        currentGettingRequests.decrementAndGet();
        LOG.error("concurrent requesting to token exceed max for " + tokenId + "-" + identifier);
        return null;
      }
      try {
        Token remoteToken = pullToken(tokenId);
        if (remoteToken != null) {
          token = new GlobalToken(remoteToken.getTokenId(), remoteToken.getTokenInfo());
          globalTokens.put(tokenId, token);
        }
      } finally {
        currentGettingRequests.decrementAndGet();
      }
    }

    // make sure remote token is equal with identifier and not expired.
    if (token != null) {
      if (!token.getTokenId().equals(identifier)) {
        LOG.warn(String.format("Token(%d) identifier is unmatched. %s : %s", tokenId, token.getTokenId(), identifier));
        tokenInfo = FAKE_TOKEN_INFO;
      } else {
        tokenInfo = token.getTokenInfo();
        // token is expired and haven't sync the latest
        if (tokenInfo.getRenewDate() < now && token.getSyncTime() <= tokenInfo
            .getRenewDate()) {
          // if token is expired, maybe the cache of global token is expired
          // try to fetch from db again
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
      final TokenAsync tokenRemover = new TokenAsync();
      tokenAsyncThread = new Thread(tokenRemover);
      tokenAsyncThread.setDaemon(true);
      tokenAsyncThread.setName(tokenRemover.toString());
      tokenAsyncThread.start();
    }
  }


  /**
   * should be called before this object is used
   */
  public void stopThreads() {
    synchronized (this) {
      running = false;
      if (tokenAsyncThread != null) {
        tokenAsyncThread.interrupt();
        try {
          tokenAsyncThread.join();
        } catch (InterruptedException e) {
          throw new RuntimeException(
              "Unable to join on token removal thread", e);
        }
      }
    }
  }

  private class TokenAsync implements Runnable {

    /**
     * handle token in queue.
     */
    private void handleQueueToken() {
      try {
        while (!tokenQueue.isEmpty()) {
          List<TokenOperation> tmpList = new ArrayList<>();
          List<Token> pushList = new ArrayList<>();
          List<Token> updateList = new ArrayList<>();
          List<Long> cancelList = new ArrayList<>();

          tokenQueue.drainTo(tmpList, AuthConfigureHolder.getUnionTokenAsyncBatch());
          for (TokenOperation operation : tmpList) {
            switch (operation.getType()) {
              case PUSH:
                pushList.add(new Token(operation.getGlobalId(), operation.getTokenIdent(), operation.getTokenInfo()));
                break;
              case UPDATE:
                Token token = new Token();
                token.setId(operation.getGlobalId());
                token.setTokenInfo(operation.getTokenInfo());
                updateList.add(token);
                break;
              case CANCEL:
                cancelList.add(operation.getGlobalId());
                break;
              default:
                break;
            }
          }
          if (!pushList.isEmpty()) {
            TokenUtil.push(pushList);
          }
          if (!updateList.isEmpty()) {
            TokenUtil.updateInfo(updateList);
          }
          if (!cancelList.isEmpty()) {
            TokenUtil.cancel(cancelList);
          }
        }
      } catch (Exception ex) {
        LOG.error("Exception in token async", ex);
      }
    }


    @Override
    public void run() {
      try {
        while (running) {
          handleQueueToken();
          // remove cache if global cache is full.
          removeExceedToken();
          TimeUnit.MILLISECONDS.sleep(AuthConfigureHolder.getUnionTokenAsyncInterval());
        }
      } catch (Throwable t) {
        LOG.error("TokenAsync thread received unexpected exception", t);
      }
    }
  }

  /**
   * Remove expired delegation tokens from cache
   */
  public void removeExpiredToken() {
    long now = System.currentTimeMillis();
    Iterator<Map.Entry<Long, GlobalToken>> iter =
            globalTokens.entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Long, GlobalToken> entry = iter.next();
      final GlobalToken token = entry.getValue();
      long renewDate = token.getTokenInfo().getRenewDate();
      if (renewDate < now && (now - token.getSyncTime()) > maxIdleTime) {
        iter.remove();
      }
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
