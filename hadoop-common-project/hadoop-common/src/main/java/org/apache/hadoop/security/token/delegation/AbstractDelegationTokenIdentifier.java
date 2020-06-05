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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.security.HadoopKerberosName;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hadoop.security.token.delegation.DelegationTokenVersionHolder.TOKEN_IDENTIFIER_SERIALIZATION_VERSION;
import static org.apache.hadoop.security.token.delegation.DelegationTokenVersionHolder.TOKEN_IDENTIFIER_TRANSMISSION_VERSION;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractDelegationTokenIdentifier 
extends TokenIdentifier {
  private static final byte VERSION = 0;
  public static final byte DEFAULT_DELEGATION_TOKEN_IDENTIFIER_VERSION = VERSION;
  public static final byte UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION = 1;

  private long id;
  private Text owner;
  private Text renewer;
  private Text realUser;
  private long issueDate;
  private long maxDate;
  private int sequenceNumber;
  private int masterKeyId = 0;
  
  public AbstractDelegationTokenIdentifier() {
    this(new Text(), new Text(), new Text());
  }
  
  public AbstractDelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    setOwner(owner);
    setRenewer(renewer);
    setRealUser(realUser);
    issueDate = 0;
    maxDate = 0;
  }

  @Override
  public abstract Text getKind();
  
  /**
   * Get the username encoded in the token identifier
   * 
   * @return the username or owner
   */
  @Override
  public UserGroupInformation getUser() {
    if ( (owner == null) || (owner.toString().isEmpty())) {
      return null;
    }
    final UserGroupInformation realUgi;
    final UserGroupInformation ugi;
    if ((realUser == null) || (realUser.toString().isEmpty())
        || realUser.equals(owner)) {
      ugi = realUgi = UserGroupInformation.createRemoteUser(owner.toString());
    } else {
      realUgi = UserGroupInformation.createRemoteUser(realUser.toString());
      ugi = UserGroupInformation.createProxyUser(owner.toString(), realUgi);
    }
    realUgi.setAuthenticationMethod(AuthenticationMethod.TOKEN);
    return ugi;
  }

  public boolean isUnion(){
    return id > 0;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public Text getOwner() {
    return owner;
  }

  public void setOwner(Text owner) {
    if (owner == null) {
      this.owner = new Text();
    } else {
      this.owner = owner;
    }
  }

  public Text getRenewer() {
    return renewer;
  }

  public void setRenewer(Text renewer) {
    if (renewer == null) {
      this.renewer = new Text();
    } else {
      HadoopKerberosName renewerKrbName = new HadoopKerberosName(renewer.toString());
      try {
        this.renewer = new Text(renewerKrbName.getShortName());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public Text getRealUser() {
    return realUser;
  }

  public void setRealUser(Text realUser) {
    if (realUser == null) {
      this.realUser = new Text();
    } else {
      this.realUser = realUser;
    }
  }

  public void setIssueDate(long issueDate) {
    this.issueDate = issueDate;
  }
  
  public long getIssueDate() {
    return issueDate;
  }
  
  public void setMaxDate(long maxDate) {
    this.maxDate = maxDate;
  }
  
  public long getMaxDate() {
    return maxDate;
  }

  public void setSequenceNumber(int seqNum) {
    this.sequenceNumber = seqNum;
  }
  
  public int getSequenceNumber() {
    return sequenceNumber;
  }

  public void setMasterKeyId(int newId) {
    masterKeyId = newId;
  }

  public int getMasterKeyId() {
    return masterKeyId;
  }

  protected static boolean isEqual(Object a, Object b) {
    return a == null ? b == null : a.equals(b);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof AbstractDelegationTokenIdentifier) {
      AbstractDelegationTokenIdentifier that = (AbstractDelegationTokenIdentifier) obj;
      return this.sequenceNumber == that.sequenceNumber 
          && this.issueDate == that.issueDate 
          && this.maxDate == that.maxDate
          && this.masterKeyId == that.masterKeyId
          && isEqual(this.owner, that.owner) 
          && isEqual(this.renewer, that.renewer)
          && isEqual(this.realUser, that.realUser);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.sequenceNumber;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    if (version != DEFAULT_DELEGATION_TOKEN_IDENTIFIER_VERSION
        && version != UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION) {
      throw new IOException("Unknown version of delegation token " +
          version);
    }
    if(version == UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION){
      id = WritableUtils.readVLong(in);
    }
    owner.readFields(in, Text.DEFAULT_MAX_LEN);
    renewer.readFields(in, Text.DEFAULT_MAX_LEN);
    realUser.readFields(in, Text.DEFAULT_MAX_LEN);
    issueDate = WritableUtils.readVLong(in);
    maxDate = WritableUtils.readVLong(in);
    sequenceNumber = WritableUtils.readVInt(in);
    masterKeyId = WritableUtils.readVInt(in);
  }

  @VisibleForTesting
  void writeImpl(DataOutput out) throws IOException {
    writeImpl(out, TOKEN_IDENTIFIER_SERIALIZATION_VERSION);
  }

  private void writeImpl(DataOutput out, int serialVersion) throws IOException {
    out.writeByte(serialVersion);
    if (serialVersion == UNION_DELEGATION_TOKEN_IDENTIFIER_VERSION) {
      WritableUtils.writeVLong(out, id);
    }
    owner.write(out);
    renewer.write(out);
    realUser.write(out);
    WritableUtils.writeVLong(out, issueDate);
    WritableUtils.writeVLong(out, maxDate);
    WritableUtils.writeVInt(out, sequenceNumber);
    WritableUtils.writeVInt(out, masterKeyId);
  }

  @Override
  public byte[] getBytes() {
    DataOutputBuffer buf = new DataOutputBuffer(4096);
    try {
      this.write(buf, TOKEN_IDENTIFIER_TRANSMISSION_VERSION);
    } catch (IOException ie) {
      throw new RuntimeException("i/o error in getBytes", ie);
    }
    return Arrays.copyOf(buf.getData(), buf.getLength());
  }

  @Override
  public void write(DataOutput out) throws IOException {
    write(out, TOKEN_IDENTIFIER_SERIALIZATION_VERSION);
  }

  private void write(DataOutput out, int serialVersion) throws IOException {
    if (owner.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new IOException("owner is too long to be serialized!");
    }
    if (renewer.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new IOException("renewer is too long to be serialized!");
    }
    if (realUser.getLength() > Text.DEFAULT_MAX_LEN) {
      throw new IOException("realuser is too long to be serialized!");
    }
    writeImpl(out, serialVersion);
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer
        .append(getKind())
        .append(" owner=").append(owner)
        .append(", renewer=").append(renewer)
        .append(", realUser=").append(realUser)
        .append(", issueDate=").append(issueDate)
        .append(", maxDate=").append(maxDate)
        .append(", sequenceNumber=").append(sequenceNumber)
        .append(", masterKeyId=").append(masterKeyId);
    if (isUnion()) {
      buffer.append(", id=").append(id);
    }
    return buffer.toString();
  }

  public static int getVersion(byte[] identifier) {
    return (identifier != null && identifier.length > 0) ? identifier[0] : -1;
  }
  /*
   * A frozen version of toString() to be used to be backward compatible.
   * When backward compatibility is not needed, use toString(), which provides
   * more info and is supposed to evolve, see HDFS-9732.
   * Don't change this method except for major revisions.
   *
   * NOTE:
   * Currently this method is used by CLI for backward compatibility.
   */
  public String toStringStable() {
    StringBuilder buffer = new StringBuilder();
    buffer
        .append("owner=").append(owner)
        .append(", renewer=").append(renewer)
        .append(", realUser=").append(realUser)
        .append(", issueDate=").append(issueDate)
        .append(", maxDate=").append(maxDate)
        .append(", sequenceNumber=").append(sequenceNumber)
        .append(", masterKeyId=").append(masterKeyId);
    return buffer.toString();
  }
}
