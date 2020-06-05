package org.apache.hadoop.security.tauth;

public class TAuthConst {

  public static final String TAUTH = "TAUTH";

  public static final String TAUTH_ALLOW_WITHOUT_LOGIN_KEY = "tauth.allow.without.login";
  public static final boolean TAUTH_ALLOW_WITHOUT_LOGIN_DEFAULT = true;

  public static final String TAUTH_REBUILD_WITHOUT_LOGIN_KEY = "tauth.rebuild.without.login";
  public static final boolean TAUTH_REBUILD__WITHOUT_LOGIN_DEFAULT = true;

  public static final String TAUTH_NOKEY_FALLBACK_ALLOWED = "tauth.no-key.fallback.allowed";
  public static final boolean TAUTH_NOKEY_FALLBACK_ALLOWED_DEFAULT = false;


  public static String TAUTH_PROTOCOL_NAME_KEY = "tauth.protocol.name";

  public static String TAUTH_SPLITTER = "---";

  public final static int VERSION = 0;
}
