/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.security.sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.tauth.TAuthSaslClient;
import org.apache.hadoop.security.tauth.TAuthSaslServer;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.SaslClient;
import javax.security.sasl.SaslClientFactory;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.apache.hadoop.security.sasl.TqAuthConst.TAUTH;

// Override 'org/apache/hadoop/security/sasl/TqFactoryImpl.class' in 'org.apache.hadoop:tq-security' to support tauth in multi-classloaders environment.
public class TqFactoryImpl implements SaslClientFactory, SaslServerFactory {
  private static final Log LOGGER = LogFactory.getLog(TqFactoryImpl.class);

  @Override
  public SaslClient createSaslClient(String[] mechanisms,
      String authorizationId, String protocol, String serverName,
      Map<String, ?> props, CallbackHandler cbh) {
    for (String mechanism : mechanisms) {
      if (TAUTH.equals(mechanism)) {
        String className = null;
        Class<?> implClass = null;
        Constructor constructor = null;
        ClassLoader classLoader = cbh.getClass().getClassLoader();
        try {
          if (cbh instanceof TqClientCallbackHandler
              // Todo: just a workaround for now,
              //  we need to find a better way to identify 'TqClientCallbackHandler' in multi-classloaders environment when different connectors use tauth at the same time.
              || TqClientCallbackHandler.class.getName().equals(cbh.getClass().getSuperclass().getName())) {
            className = TqSaslClient.class.getName();
            implClass = Class.forName(className, true, classLoader);
            constructor = implClass.getConstructor(String.class, String.class, String.class, Map.class, CallbackHandler.class);
            return (SaslClient) constructor.newInstance(authorizationId, protocol, serverName, props, cbh);
          }

          className = TAuthSaslClient.class.getName();
          implClass = Class.forName(className, true, classLoader);
          constructor = implClass.getConstructor(String.class, String.class, Map.class, CallbackHandler.class);
          return (SaslClient) constructor.newInstance(protocol, serverName, props, cbh);
        }
        catch (ClassNotFoundException e) {
          LOGGER.error("Cannot load class " + className, e);
        }
        catch (NoSuchMethodException e) {
          LOGGER.error("Cannot construct class " + className, e);
        }
        catch (InstantiationException e) {
          LOGGER.error("Cannot instantiate class " + className, e);
        }
        catch (IllegalAccessException e) {
          LOGGER.error("Cannot access class " + className, e);
        }
        catch (InvocationTargetException e) {
          LOGGER.error("Cannot invocate class " + className, e);
        }
      }
    }
    return null;
  }

  // We keep this method unchanged because we use tauth as a client.
  @Override
  public SaslServer createSaslServer(String mechanism, String protocol,
      String serverName, Map<String, ?> props, CallbackHandler cbh) {
    if (TAUTH.equals(mechanism)) {
      if (cbh instanceof TqServerCallbackHandler) {
        return new TqSaslServer(protocol, serverName, props, cbh);
      }
      return new TAuthSaslServer(protocol, serverName, props, cbh);
    }
    return null;
  }

  @Override
  public String[] getMechanismNames(Map<String, ?> props) {
    return new String[]{TAUTH};
  }
}
