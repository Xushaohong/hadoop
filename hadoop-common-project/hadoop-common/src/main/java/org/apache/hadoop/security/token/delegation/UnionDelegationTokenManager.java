package org.apache.hadoop.security.token.delegation;

import com.tencent.tdw.security.beans.TokenIdent;
import com.tencent.tdw.security.beans.TokenInformation;

import java.net.InetAddress;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.AbstractAuthTokenDelegationManager;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSecretManager.DelegationTokenInformation;

public class UnionDelegationTokenManager<TOKEN_IDENT extends AbstractDelegationTokenIdentifier>
    extends AbstractAuthTokenDelegationManager<TOKEN_IDENT, DelegationTokenInformation> {

  public UnionDelegationTokenManager(long maxIdleTime, int maxTokenCacheNum, long tokenRemoverScanInterval) {
    super(maxIdleTime, maxTokenCacheNum, tokenRemoverScanInterval);
  }

  private static final byte [] PASSWORD_PLACEHOLDER = "nopassword".getBytes();
  static final TokenInformation TOKEN_INFO_PLACEHOLDER =
      new TokenInformation(0, Base64.encodeBase64String(PASSWORD_PLACEHOLDER),
          "temporary", "placeholder");
  private static String ISSUER;

  static {
    try {
      ISSUER = InetAddress.getLocalHost().getHostAddress();
    } catch (Exception e) {
      ISSUER = "unknown";
    }
  }

  private AbstractAuthTokenDelegationManager.Converter<TOKEN_IDENT, DelegationTokenInformation> converter =
      new Converter<TOKEN_IDENT, DelegationTokenInformation>() {
        @Override
        public TokenIdent toId(TOKEN_IDENT tId) {
          return new TokenIdent(tId.getOwner().toString(), tId.getRenewer().toString(),
              tId.getRealUser().toString(), tId.getIssueDate(), tId.getMaxDate(), tId.getKind().toString());
        }

        @Override
        public DelegationTokenInformation toInfo(TokenInformation tInfo) {
          return new DelegationTokenInformation(tInfo.getRenewDate(),
              Base64.decodeBase64(tInfo.getPassword()), tInfo.getTrackingId());
        }

        @Override
        public TokenInformation toInfo(DelegationTokenInformation tInfo) {
          return new TokenInformation(tInfo.getRenewDate(),
              Base64.encodeBase64String(tInfo.getPassword()),
              tInfo.getTrackingId(), ISSUER);
        }
      };

  @Override
  public Converter<TOKEN_IDENT, DelegationTokenInformation> getConverter() {
    return converter;
  }

}
