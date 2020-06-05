package org.apache.hadoop.security.token;

import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TqTokenIdentifierTest {

  @Test
  public void testInit() throws IOException {
    String tokenAuth = "token.eyJpIjoxLCJlIjozLCJvIjoidXNlcjAiLCJyIjoicmVhbFVzZXIwIiwicnUiOiJyZWFsVXNlcjAiLCJpdCI6MTU2OTQ4Mzc2NzEwNywibXQiOjg2NDAwMDAwLCJrIjoiYXV0aG9yaXphdGlvbiIsInMiOiJBdXRoLVRlc3QifQ==|ak+NaonmX4wwvg0NHWd7KRfLT6o=";
    Map<String, String> parms = new HashMap<>();
    parms.put("A", "a");
    parms.put("B", "b");
    TqTokenIdentifier identifier = new TqTokenIdentifier(tokenAuth, parms);
    Assert.assertNotNull(identifier.getPassword());
    Assert.assertNotNull(identifier.getKind());
    Assert.assertTrue(identifier.getMaxDate() > 0);
    Assert.assertTrue(identifier.getIssueDate() > 0);
    Assert.assertNotNull(identifier.getRawToken());
    Assert.assertNotNull(identifier.getOwner());
    Assert.assertNotNull(identifier.getUser());
    Assert.assertNotNull(identifier.getPassword());
    byte[] bytes = identifier.getBytes();
    Assert.assertNotNull(bytes);

    TqTokenIdentifier newIdentifier = new TqTokenIdentifier();
    DataInput input = new DataInputStream(new BufferedInputStream(new ByteArrayInputStream(bytes)));
    newIdentifier.readFields(input);
    Assert.assertArrayEquals(newIdentifier.getBytes(), bytes);
    Assert.assertEquals(newIdentifier.getAuthentication(), identifier.getAuthentication());
    Assert.assertEquals(tokenAuth, identifier.getAuthentication());
  }

  @Test
  public void testToken() throws IOException {
    String tokenAuth = "token.eyJpIjoxLCJlIjozLCJvIjoidXNlcjAiLCJyIjoicmVhbFVzZXIwIiwicnUiOiJyZWFsVXNlcjAiLCJpdCI6MTU2OTQ4Mzc2NzEwNywibXQiOjg2NDAwMDAwLCJrIjoiYXV0aG9yaXphdGlvbiIsInMiOiJBdXRoLVRlc3QifQ==|ak+NaonmX4wwvg0NHWd7KRfLT6o=";
    Map<String, String> parms = new HashMap<>();
    parms.put("A", "a");
    parms.put("B", "b");
    TqTokenIdentifier identifier = new TqTokenIdentifier(tokenAuth, parms);

    Token<TqTokenIdentifier> token = new Token<>(identifier.getBytes(), identifier.getPassword(),
        identifier.getKind(), TqTokenIdentifier.TQ_TOKEN);
    TqTokenIdentifier tqTokenIdentifier = token.decodeIdentifier();
    Assert.assertArrayEquals(tqTokenIdentifier.getBytes(), identifier.getBytes());
    DataOutputBuffer output = new DataOutputBuffer();
    token.write(output);
    byte[] tokenBytes = output.getData();
    DataInputBuffer input = new DataInputBuffer();
    input.reset(tokenBytes, tokenBytes.length);
    Token<TqTokenIdentifier> newToken = new Token<>();
    newToken.readFields(input);
    Assert.assertArrayEquals(newToken.getIdentifier(), token.getIdentifier());
    Assert.assertEquals(newToken.getService(), token.getService());
    Assert.assertEquals(newToken.getService(), token.getService());
    Assert.assertEquals(newToken.getKind(), token.getKind());
    Assert.assertArrayEquals(newToken.getPassword(), token.getPassword());

    Assert.assertEquals(newToken.decodeIdentifier().getAuthentication(), identifier.getAuthentication());
    Assert.assertEquals(tokenAuth, identifier.getAuthentication());
    Assert.assertTrue(new String(newToken.getIdentifier()).startsWith(TqTokenIdentifier.TQ_TOKEN.toString()));
  }
}
