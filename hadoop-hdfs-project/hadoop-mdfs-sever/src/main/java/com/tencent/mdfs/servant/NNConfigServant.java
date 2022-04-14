package com.tencent.mdfs.servant;

import com.qq.cloud.taf.common.annotation.JceService;

/**
 * Generated code, DO NOT modify it!
 * @author jce2java
 */
@JceService
public interface NNConfigServant {
    public java.util.Map<String, java.util.List<String>> getAllNameNodes();
    public java.util.List<java.util.Map<String,String>> getAllNameNodeVersion();
}
