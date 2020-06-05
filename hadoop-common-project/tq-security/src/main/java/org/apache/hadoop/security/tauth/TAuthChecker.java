package org.apache.hadoop.security.tauth;

import com.google.common.base.Preconditions;
import com.tencent.tdw.security.authentication.tauth.AuthenticationChecker;

import javax.security.auth.Subject;
import java.security.AccessControlContext;
import java.security.AccessController;

public class TAuthChecker implements AuthenticationChecker {
  public TAuthChecker() {
  }

  @Override
  public void check(String userName) throws SecurityException {
    AccessControlContext context = AccessController.getContext();
    Subject subject = Subject.getSubject(context);
    TAuthLoginModule.TAuthPrincipal principal = TAuthLoginModule.TAuthPrincipal.getFromSubject(subject);
    Preconditions.checkNotNull(principal, TAuthLoginModule.TAuthPrincipal.class.getName());
    Preconditions.checkState(principal.getName().equals(userName),
        "User not matched:[principal:" + principal.getName() + ", auth user:" + userName);
  }
}
