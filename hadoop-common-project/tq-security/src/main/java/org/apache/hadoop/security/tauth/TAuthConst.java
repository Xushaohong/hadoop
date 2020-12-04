package org.apache.hadoop.security.tauth;

public class TAuthConst {

  public static final String TAUTH = "TAUTH";

  public static final String TAUTH_NOKEY_FALLBACK_ALLOWED = "tauth.no-key.fallback.allowed";
  public static final boolean TAUTH_NOKEY_FALLBACK_ALLOWED_DEFAULT = false;

  //是否允许server端配置为tauth，目前只用于journalNode等配置了TAUTH认证，但无需使用秘钥的场景下
  public static final String TAUTH_NOKEY_SERVER_ALLOWED = "tauth.no-key.server.allowed";
  public static final boolean TAUTH_NOKEY_SERVER_ALLOWED_DEFAULT = true;

  public static String TAUTH_PROTOCOL_NAME_KEY = "tauth.protocol.name";

  public static String TAUTH_SPLITTER = "---";

  public final static int VERSION = 0;
}
