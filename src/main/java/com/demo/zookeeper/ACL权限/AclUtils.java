package com.demo.zookeeper.ACL权限;
 
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
 
public class AclUtils {
 
    public static String getDigestUserPwd(String id) throws Exception {
        // 加密明文密码
        return DigestAuthenticationProvider.generateDigest(id);
    }
}
