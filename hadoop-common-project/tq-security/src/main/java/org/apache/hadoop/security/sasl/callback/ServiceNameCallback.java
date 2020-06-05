package org.apache.hadoop.security.sasl.callback;

import javax.security.auth.callback.Callback;

public class ServiceNameCallback implements Callback {
  private String serviceName;

  public ServiceNameCallback() {
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getServiceName() {
    return serviceName;
  }
}
