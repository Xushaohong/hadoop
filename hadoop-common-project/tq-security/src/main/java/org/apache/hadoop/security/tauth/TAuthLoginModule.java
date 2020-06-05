package org.apache.hadoop.security.tauth;

import com.tencent.tdw.security.Tuple;
import com.tencent.tdw.security.authentication.LocalKeyManager;
import com.tencent.tdw.security.utils.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.Utils;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.Serializable;
import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class TAuthLoginModule implements LoginModule {

  private static final Log LOG = LogFactory.getLog(TAuthLoginModule.class);

  private Subject subject;
  private Map<String, ?> options;
  private String principal;
  private String keyPath;
  private String masterKey;
  public final static String TAUTH_PRINCIPAL = "pricipal";
  public final static String TAUTH_KEY = "tauthKey";
  public final static String TAUTH_KEY_PATH = "tauthKeyPath";
  public final static String TAUTH_CUSTOM_ENABLE = "tq.tauth.custom.enable";
  public final static String TAUTH_CUSTOM_USER_KEY_PATH = "tq.tauth.custom.key.path";
  public final static String TAUTH_CUSTOM_USER_KEY = "tq.tauth.custom.key";
  public final static String TAUTH_CUSTOM_USER_PRINCIPAL = "tq.tauth.custom.principal";
  public final static String OS_PRINCIPAL_CLASS = "os.principal.class";
  private static final boolean CUSTOM_ENABLE = Utils.getSystemPropertyOrEnvVar(TAUTH_CUSTOM_ENABLE, true);
  private static final String CUSTOM_USER = Utils.getSystemPropertyOrEnvVar(TAUTH_CUSTOM_USER_PRINCIPAL);
  private static final String CUSTOM_KEY = Utils.getSystemPropertyOrEnvVar(TAUTH_CUSTOM_USER_KEY);
  private static final String CUSTOM_KEY_PATH = Utils.getSystemPropertyOrEnvVar(TAUTH_CUSTOM_USER_KEY_PATH);

  @Override
  public void initialize(Subject subject, CallbackHandler callbackHandler,
      Map<String, ?> sharedState, Map<String, ?> options) {
    this.subject = subject;
    this.options = options;
  }

  private <T extends Principal> T getCanonicalUser(Class<T> cls) {
    for (T user : subject.getPrincipals(cls)) {
      return user;
    }
    return null;
  }

  @Override
  public boolean login() throws LoginException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(TAuthConst.TAUTH + " login");
    }
    this.principal = (String) options.get(TAUTH_PRINCIPAL);
    this.keyPath = (String) options.get(TAUTH_KEY_PATH);
    this.masterKey = (String) options.get(TAUTH_KEY);
    return true;
  }

  @Override
  public boolean commit() throws LoginException {
    if (LOG.isDebugEnabled()) {
      LOG.debug(TAuthConst.TAUTH + " commit");
    }

    if (CUSTOM_ENABLE) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Custom " + TAuthConst.TAUTH +
            "{user=" + CUSTOM_USER + ", key=" + CUSTOM_KEY + ", keyPath=" + CUSTOM_KEY_PATH + "}");
      }
      if (Utils.isNotNullOrEmpty(CUSTOM_USER)) {
        principal = CUSTOM_USER;
      }
      if (Utils.isNotNullOrEmpty(CUSTOM_KEY)) {
        masterKey = CUSTOM_KEY;
      }
      if (Utils.isNotNullOrEmpty(CUSTOM_KEY_PATH)) {
        keyPath = CUSTOM_KEY_PATH;
      }
    }

    if (principal == null) {
      final Class<?> principalClass = (Class<?>) options.get(OS_PRINCIPAL_CLASS);
      if (principalClass != null && Principal.class.isAssignableFrom(principalClass)) {
        Principal osUser = getCanonicalUser((Class<Principal>) principalClass);
        if (LOG.isDebugEnabled()) {
          LOG.debug("using local user:" + osUser);
        }
        if (osUser != null) {
          principal = osUser.getName();
        }
      }
    }

    if (principal == null) {
      LOG.error("User name for " + subject + " is not found");
      throw new LoginException("User name is not found");
    }

    Tuple<TAuthPrincipal, TAuthCredential> tauthTuple = buildTAuthPrincipalAndCredential(principal, masterKey, keyPath);
    subject.getPrincipals().add(tauthTuple._1());
    subject.getPrivateCredentials().add(tauthTuple._2());
    if (LOG.isDebugEnabled()) {
      LOG.debug(TAuthConst.TAUTH + " principal:" + tauthTuple._1() + " ,credential:" + tauthTuple._2());
    }
    return true;
  }

  public static Tuple<TAuthPrincipal, TAuthCredential> buildCustomPrincipalAndCredentialTupleIfEnabled() {
    return buildCustomPrincipalAndCredentialTupleIfEnabled(System.getProperty("user.name"));
  }

  public static Tuple<TAuthPrincipal, TAuthCredential> buildCustomPrincipalAndCredentialTupleIfEnabled(String currentUser) {
    if (CUSTOM_ENABLE) {
      if (CUSTOM_USER != null) {
        currentUser = CUSTOM_USER;
      }
      return buildTAuthPrincipalAndCredential(currentUser, CUSTOM_KEY, CUSTOM_KEY_PATH);
    }
    return null;
  }

  public static Tuple<TAuthPrincipal, TAuthCredential> buildAuthPrincipalAndCredential(String principal) {
    return buildTAuthPrincipalAndCredential(principal, null, null);
  }

  public static Tuple<TAuthPrincipal, TAuthCredential> buildTAuthPrincipalAndCredential(String principal, String masterKey, String keyPath) {
    TAuthPrincipal tauthPrincipal = new TAuthPrincipal(principal);
    TAuthCredential credential = null;
    if (Utils.isNotNullOrEmpty(masterKey)) {
      credential = TAuthCredential.buildWithMasterKey(masterKey);
    }
    if (Utils.isNotNullOrEmpty(keyPath) && FileUtils.exists(keyPath)) {
      credential = TAuthCredential.buildWithKeyFilePath(keyPath);
    }

    if (credential == null) {
      credential = TAuthCredential.buildWithPrinciple(principal);
    }
    return Tuple.of(tauthPrincipal, credential);
  }


  @Override
  public boolean abort() throws LoginException {
    logout();
    return true;
  }

  @Override
  public boolean logout() throws LoginException {
    TAuthPrincipal.removeIfPresent(subject);
    TAuthCredential.removeIfPresent(subject);

    if (LOG.isDebugEnabled()) {
      LOG.debug(TAuthConst.TAUTH + " logout");
    }
    return true;
  }

  public static class TAuthPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = -2951667807823493630L;

    private final String name;

    public TAuthPrincipal(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public String toString() {
      return "name:" + name;
    }

    public static TAuthPrincipal getFromSubject(Subject subject) {
      for (TAuthPrincipal principal : subject.getPrincipals(TAuthPrincipal.class)) {
        return principal;
      }
      return null;
    }

    public static void removeIfPresent(Subject subject){
      Iterator<Principal> iterator = subject.getPrincipals().iterator();
      while (iterator.hasNext()) {
        Object o = iterator.next();
        if (o instanceof TAuthPrincipal) {
          iterator.remove();
        }
      }
    }
  }

  public static class TAuthCredential {
    private final String principal;
    private final String masterKey;
    private final String keyPath;

    TAuthCredential(String principal, String masterKey, String keyPath) {
      this.principal = principal;
      this.masterKey = masterKey;
      this.keyPath = keyPath;
    }

    private transient LocalKeyManager localKeyManager;

    public String getKeyPath() {
      return keyPath;
    }

    public String getMasterKey() {
      return masterKey;
    }

    public LocalKeyManager getLocalKeyManager() {
      if (localKeyManager == null) {
        if (Utils.isNotNullOrEmpty(keyPath)) {
          localKeyManager = LocalKeyManager.generateByDir(keyPath);
        } else if (Utils.isNotNullOrEmpty(masterKey)) {
          localKeyManager = LocalKeyManager.generateByKeys(Collections.singletonList(masterKey));
        } else {
          localKeyManager = LocalKeyManager.generateByUser(principal);
        }
      }
      return localKeyManager;
    }

    public static TAuthCredential buildWithKeyFilePath(String keyPath) {
      return new TAuthCredential(null, null, keyPath);
    }

    public static TAuthCredential buildWithMasterKey(String masterKey) {
      return new TAuthCredential(null, masterKey, null);
    }

    public static TAuthCredential buildWithPrinciple(String principal) {
      return new TAuthCredential(principal, null, null);
    }

    public static TAuthCredential getFromSubject(Subject subject) {
      for (TAuthCredential credential : subject.getPrivateCredentials(TAuthCredential.class)) {
        return credential;
      }
      return null;
    }

    public static void removeIfPresent(Subject subject){
      Iterator<Object> iterator = subject.getPrivateCredentials().iterator();
      while (iterator.hasNext()) {
        Object o = iterator.next();
        if (o instanceof TAuthCredential) {
          iterator.remove();
        }
      }
    }

    @Override
    public String toString() {
      return "TAuthCredential{" +
          "principal='" + principal + '\'' +
          ", specified masterKey='" + (masterKey != null) + '\'' +
          ", keyPath='" + keyPath + '\'' +
          '}';
    }
  }
}


