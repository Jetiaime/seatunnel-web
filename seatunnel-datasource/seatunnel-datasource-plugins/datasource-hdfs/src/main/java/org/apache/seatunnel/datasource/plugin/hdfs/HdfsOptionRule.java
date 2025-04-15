package org.apache.seatunnel.datasource.plugin.hdfs;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.util.Arrays;
import java.util.Map;

public class HdfsOptionRule {

    public static final Option<String> FS_DEFAULT_FS =
            Options.key("fs.defaultFS")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The hadoop cluster address that start with hdfs://, "
                                    + "for example: hdfs://hadoopcluster");

    public static final Option<String> HDFS_SITE_PATH =
            Options.key("hdfs_site_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "\tThe path of hdfs-site.xml, used to load ha configuration of `namenodes`");

    public static final Option<String> REMOTE_USER =
            Options.key("remote_user")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The login user used to connect to hadoop login name. It is intended to be used for remote users in RPC, "
                                    + "it won't have any credentials.");

    public static final Option<String> KRB5_PATH =
            Options.key("krb5_path")
                    .stringType()
                    .defaultValue("/etc/krb5.conf")
                    .withDescription("The krb5 path of kerberos");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The principal of kerberos");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The keytab path of kerberos");

    public static OptionRule optionRule() {
        return OptionRule.builder()
                .required(FS_DEFAULT_FS)
                .optional(HDFS_SITE_PATH)
                .optional(REMOTE_USER)
                .optional(KRB5_PATH)
                .optional(KERBEROS_PRINCIPAL)
                .optional(KERBEROS_KEYTAB_PATH)
                .build();
    }

    public static final Option<String> PATH =
            Options.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The source file path.");

    public static final Option<FileFormat> FILE_FORMAT_TYPE =
            Options.key("file_format_type")
                    .objectType(FileFormat.class)
                    .noDefaultValue()
                    .withDescription(
                            "We supported as the following file types:text csv parquet orc json excel xml binary."
                                    + "Please note that, The final file name will end with the file_format's suffix, "
                                    + "the suffix of the text file is txt.");

    public static final Option<String> DELIMITER =
            Options.key("delimiter")
                    .stringType()
                    .defaultValue("\\001")
                    .withDescription(
                            "Field delimiter, used to tell connector how to slice and dice fields when reading text files. "
                                    + "default \\001, the same as hive's default delimiter");

    public static final Option<Boolean> PARSE_PARTITION_FROM_PATH =
            Options.key("parse_partition_from_path")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Control whether parse the partition keys and values from file path. "
                                    + "For example if you read a file from path hdfs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26. "
                                    + "Every record data from file will be added these two fields:[name:tyrantlucifer,age:26]."
                                    + "Tips:Do not define partition fields in schema option.");

    public static final Option<String> DATE_FORMAT =
            Options.key("date_format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd")
                    .withDescription(
                            "Date type format, used to tell connector how to convert string to date, "
                                    + "supported as the following formats:yyyy-MM-dd yyyy.MM.dd yyyy/MM/dd default yyyy-MM-dd."
                                    + "Date type format, used to tell connector how to convert string to date, "
                                    + "supported as the following formats:yyyy-MM-dd yyyy.MM.dd yyyy/MM/dd "
                                    + "default yyyy-MM-dd");

    public static final Option<String> DATETIME_FORMAT =
            Options.key("datetime_format")
                    .stringType()
                    .defaultValue("yyyy-MM-dd HH:mm:ss")
                    .withDescription(
                            "Datetime type format, used to tell connector how to convert string to datetime, "
                                    + "supported as the following formats:yyyy-MM-dd HH:mm:ss yyyy.MM.dd HH:mm:ss yyyy/MM/dd HH:mm:ss yyyyMMddHHmmss ."
                                    + "default yyyy-MM-dd HH:mm:ss");

    public static final Option<String> TIME_FORMAT =
            Options.key("time_format")
                    .stringType()
                    .defaultValue("HH:mm:ss")
                    .withDescription(
                            "Time type format, used to tell connector how to convert string to time, "
                                    + "supported as the following formats:HH:mm:ss HH:mm:ss.SSS.default HH:mm:ss");

    public static final Option<Map<String, String>> SCHEMA =
            Options.key("schema")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("the schema fields of upstream data");

    public static OptionRule metadataRule() {
        return OptionRule.builder()
                .required(PATH, FILE_FORMAT_TYPE)
                .conditional(FILE_FORMAT_TYPE, FileFormat.TEXT, DELIMITER)
                .conditional(
                        FILE_FORMAT_TYPE,
                        Arrays.asList(FileFormat.TEXT, FileFormat.JSON, FileFormat.CSV),
                        SCHEMA)
                .optional(PARSE_PARTITION_FROM_PATH)
                .optional(DATE_FORMAT)
                .optional(DATETIME_FORMAT)
                .optional(TIME_FORMAT)
                .build();
    }

    public enum FileFormat {
        CSV("csv"),
        TEXT("txt"),
        PARQUET("parquet"),
        ORC("orc"),
        JSON("json"),
        EXCEL("excel"),
        XML("xml"),
        BINARY("binary");

        private final String type;

        FileFormat(String type) {
            this.type = type;
        }
    }
}
