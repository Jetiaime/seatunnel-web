package org.apache.seatunnel.datasource.plugin.hdfs;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.datasource.plugin.api.DataSourcePluginException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class HdfsConfiguration {

    public static Configuration getConfiguration(Map<String, String> hdfsOptions) {
        checkNotNull(
                hdfsOptions.get(HdfsOptionRule.FS_DEFAULT_FS.key()),
                "Hdfs DefaultFS cannot be null");
        String kerberosPrincipal = hdfsOptions.get(HdfsOptionRule.KERBEROS_PRINCIPAL.key());
        String kerberosKrb5ConfPath = hdfsOptions.get(HdfsOptionRule.KRB5_PATH.key());
        String kerberosKeytabPath = hdfsOptions.get(HdfsOptionRule.KERBEROS_KEYTAB_PATH.key());
        String hdfsSitePath = hdfsOptions.get(HdfsOptionRule.HDFS_SITE_PATH.key());

        Configuration hadoopConf = new Configuration();

        hadoopConf.set("fs.defaultFS", hdfsOptions.get(HdfsOptionRule.FS_DEFAULT_FS.key()));

        if (StringUtils.isNotEmpty(kerberosKrb5ConfPath)) {
            // if this property is not set, default environment krb5.conf path is used
            System.setProperty("java.security.krb5.conf", kerberosKrb5ConfPath);
        }
        try {
            if (StringUtils.isNotEmpty(hdfsSitePath)) {
                hadoopConf.addResource(new File(hdfsSitePath).toURI().toURL());
            }
            if (StringUtils.isNotEmpty(kerberosPrincipal)) {
                // login Kerberos
                doKerberosAuthentication(hadoopConf, kerberosPrincipal, kerberosKeytabPath);
            }
        } catch (Exception e) {
            String errorMsg = String.format("Hdfs configuration error: %s", e.getMessage());
            log.error(ExceptionUtils.getMessage(e));
            throw new DataSourcePluginException(errorMsg, e);
        }

        return hadoopConf;
    }

    public static void doKerberosAuthentication(
            Configuration configuration, String principal, String keytabPath) {
        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
            log.warn(
                    "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
                    principal,
                    keytabPath);
        } else {
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                log.info(
                        "Start Kerberos authentication using principal {} and keytab {}",
                        principal,
                        keytabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                log.info("Kerberos authentication successful");
            } catch (IOException e) {
                throw new DataSourcePluginException(
                        "check hive connectivity failed, " + e.getMessage(), e);
            }
        }
    }
}
