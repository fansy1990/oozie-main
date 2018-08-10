/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.action.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.XLog;

import java.io.IOException;

public class CredentialsProvider {
    Credentials cred;
    String type;
    public static final String CRED_KEY = "oozie.credentials.credentialclasses";
    private static final XLog LOG = XLog.getLog(CredentialsProvider.class);

    public CredentialsProvider(String type) {
        this.type = type;
        this.cred = null;
        LOG.debug("Credentials Provider is created for Type: " + type);
    }

    /**
     * Create Credential object
     *
     * @return Credential object
     * @throws Exception
     */
    public Credentials createCredentialObject() throws Exception {
        String type;
        String classname;
        for (String function : ConfigurationService.getStrings(CRED_KEY)) {
            function = Trim(function);
            LOG.debug("Creating Credential class for : " + function);
            String[] str = function.split("=");
            if (str.length > 0) {
                type = str[0];
                classname = str[1];
                if (classname != null) {
                    LOG.debug("Creating Credential type : '" + type + "', class Name : '" + classname + "'");
                    if (this.type.equalsIgnoreCase(str[0])) {
                        Class<?> klass = null;
                        try {
                            klass = Thread.currentThread().getContextClassLoader().loadClass(classname);
                        }
                        catch (ClassNotFoundException ex) {
                            LOG.warn("Exception while loading the class", ex);
                            throw ex;
                        }

                        cred = (Credentials) ReflectionUtils.newInstance(klass, null);
                    }
                }
            }
        }
        return cred;
    }

    /**
     * Relogs into Kerberos using the Keytab for the Oozie server user.  This should be called before attempting to get delegation
     * tokens via {@link Credentials} implementations to ensure that the Kerberos credentials are current and won't expire too soon.
     *
     * @throws IOException
     */
    public static void ensureKerberosLogin() throws IOException {
        LOG.debug("About to relogin from keytab");
        UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
        LOG.debug("Relogin from keytab successful");
    }

    /**
     * To trim string
     *
     * @param str
     * @return trim string
     */
    public String Trim(String str) {
        if (str != null) {
            str = str.replaceAll("\\n", "");
            str = str.replaceAll("\\t", "");
            str = str.trim();
        }
        return str;
    }
}