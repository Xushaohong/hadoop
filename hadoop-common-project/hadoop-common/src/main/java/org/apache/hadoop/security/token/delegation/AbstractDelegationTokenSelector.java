/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.security.token.delegation;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.tencent.tdw.security.utils.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Utils;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.security.token.TqTokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Look through tokens to find the first delegation token that matches the
 * service and return it.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Evolving
public 
class AbstractDelegationTokenSelector<TokenIdent 
extends AbstractDelegationTokenIdentifier> 
    implements TokenSelector<TokenIdent> {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractDelegationTokenSelector.class);
  private Text kindName;
  private boolean unionTokenEnable;
  private boolean tqTokenEnable;
  private boolean filterEnable;
  
  protected AbstractDelegationTokenSelector(Text kindName) {
    this.kindName = kindName;
    this.unionTokenEnable = Utils.getSystemPropertyOrEnvVar(
        String.format("tq.%s.union.token.enable", kindName.toString()), true);
    this.tqTokenEnable = Utils.getSystemPropertyOrEnvVar(
        String.format("tq.%s.token.enable", kindName.toString()), true);
    this.filterEnable = Utils.getSystemPropertyOrEnvVar(
        String.format("tq.%s.token.filter.enable", kindName.toString()), true);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Token<TokenIdent> selectToken(Text service,
      Collection<Token<? extends TokenIdentifier>> tokens) {
    if (service == null || tokens == null || tokens.isEmpty()) {
      return null;
    }
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (kindName.equals(token.getKind())
          && service.equals(token.getService())) {
        return (Token<TokenIdent>) token;
      }
    }
    // not found corresponding token, try found union token
    if (unionTokenEnable) {
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (kindName.equals(token.getKind())) {
          if (AbstractDelegationTokenIdentifier.getVersion(token.getIdentifier())
              == AbstractDelegationTokenIdentifier.UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION) {
            TokenIdent tokenIdent;
            try {
              tokenIdent = (TokenIdent) token.decodeIdentifier();
            } catch (IOException e) {
              LOG.warn("Failed to decode identifier : " + token, e);
              continue;
            }
            if (tokenIdent != null && tokenIdent.isUnion()) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Use Union token for " + service + " ：" + token);
              }
              return (Token<TokenIdent>) token;
            }
          }
        }
      }
    }
    //try found from tq token
    if (tqTokenEnable) {
      for (Token<? extends TokenIdentifier> token : tokens) {
        if (token.getKind().equals(TqTokenIdentifier.TQ_TOKEN)) {
          boolean filter = true;
          if (filterEnable) {
            try {
              TqTokenIdentifier tokenIdentifier = (TqTokenIdentifier) token.decodeIdentifier();
              String ignoreStr = tokenIdentifier.getProps().get("ignore");
              if (!StringUtils.isBlank(ignoreStr)) {
                List<String> ignoreList = StringUtils.getStringCollection(ignoreStr);
                for (String ignore : ignoreList) {
                  if (service.toString().startsWith(ignore)) {
                    filter = false;
                  }
                }
              }
            } catch (IOException e) {
              LOG.warn("Failed to decode identifier : " + token, e);
              continue;
            }
          }
          if (filter) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Use tq token for " + service + " ：" + token);
            }
            return (Token<TokenIdent>) token;
          }
        }
      }
    }
    return null;
  }
}
