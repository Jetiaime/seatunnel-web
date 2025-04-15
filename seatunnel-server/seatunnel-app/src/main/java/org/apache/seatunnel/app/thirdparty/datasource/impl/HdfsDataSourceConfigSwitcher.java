package org.apache.seatunnel.app.thirdparty.datasource.impl;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.app.domain.request.connector.BusinessMode;
import org.apache.seatunnel.app.domain.request.job.DataSourceOption;
import org.apache.seatunnel.app.domain.request.job.SelectTableFields;
import org.apache.seatunnel.app.domain.response.datasource.VirtualTableDetailRes;
import org.apache.seatunnel.app.dynamicforms.FormStructure;
import org.apache.seatunnel.app.thirdparty.datasource.AbstractDataSourceConfigSwitcher;
import org.apache.seatunnel.app.thirdparty.datasource.DataSourceConfigSwitcher;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.datasource.plugin.hdfs.HdfsOptionRule;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
@AutoService(DataSourceConfigSwitcher.class)
public class HdfsDataSourceConfigSwitcher extends AbstractDataSourceConfigSwitcher {

    public HdfsDataSourceConfigSwitcher() {}

    @Override
    public String getDataSourceName() {
        return "Hdfs";
    }

    @Override
    public FormStructure filterOptionRule(
            String connectorName,
            OptionRule dataSourceOptionRule,
            OptionRule virtualTableOptionRule,
            BusinessMode businessMode,
            PluginType pluginType,
            OptionRule connectorOptionRule,
            List<RequiredOption> addRequiredOptions,
            List<Option<?>> addOptionalOptions,
            List<String> excludedKeys) {
        excludedKeys.add(HdfsOptionRule.PATH.key());
        if (PluginType.SOURCE.equals(pluginType)) {
            excludedKeys.add(HdfsOptionRule.SCHEMA.key());
        }

        return super.filterOptionRule(
                connectorName,
                dataSourceOptionRule,
                virtualTableOptionRule,
                businessMode,
                pluginType,
                connectorOptionRule,
                addRequiredOptions,
                addOptionalOptions,
                excludedKeys);
    }

    @Override
    public Config mergeDatasourceConfig(
            Config dataSourceInstanceConfig,
            VirtualTableDetailRes virtualTableDetail,
            DataSourceOption dataSourceOption,
            SelectTableFields selectTableFields,
            BusinessMode businessMode,
            PluginType pluginType,
            Config connectorConfig) {

        if (PluginType.SOURCE.equals(pluginType)) {
            connectorConfig =
                    connectorConfig
                            .withValue(
                                    HdfsOptionRule.SCHEMA.key(),
                                    KafkaKingbaseDataSourceConfigSwitcher.SchemaGenerator
                                            .generateSchemaBySelectTableFields(
                                                    virtualTableDetail, selectTableFields)
                                            .root())
                            .withValue(
                                    HdfsOptionRule.PATH.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.PATH.key())))
                            .withValue(
                                    HdfsOptionRule.FILE_FORMAT_TYPE.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.FILE_FORMAT_TYPE.key())))
                            .withValue(
                                    HdfsOptionRule.PARSE_PARTITION_FROM_PATH.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(
                                                            HdfsOptionRule.PARSE_PARTITION_FROM_PATH
                                                                    .key())))
                            .withValue(
                                    HdfsOptionRule.DATE_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.DATE_FORMAT.key())))
                            .withValue(
                                    HdfsOptionRule.DATETIME_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.DATETIME_FORMAT.key())))
                            .withValue(
                                    HdfsOptionRule.TIME_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.TIME_FORMAT.key())));
        } else if (PluginType.SINK.equals(pluginType)) {
            if (virtualTableDetail.getDatasourceProperties().get(HdfsOptionRule.PATH.key())
                    == null) {
                throw new IllegalArgumentException("S3 virtual table path is null");
            }
            connectorConfig =
                    connectorConfig
                            .withValue(
                                    HdfsOptionRule.PATH.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.PATH.key())))
                            .withValue(
                                    HdfsOptionRule.FILE_FORMAT_TYPE.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.FILE_FORMAT_TYPE.key())))
                            .withValue(
                                    HdfsOptionRule.PARSE_PARTITION_FROM_PATH.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(
                                                            HdfsOptionRule.PARSE_PARTITION_FROM_PATH
                                                                    .key())))
                            .withValue(
                                    HdfsOptionRule.DATE_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.DATE_FORMAT.key())))
                            .withValue(
                                    HdfsOptionRule.DATETIME_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.DATETIME_FORMAT.key())))
                            .withValue(
                                    HdfsOptionRule.TIME_FORMAT.key(),
                                    ConfigValueFactory.fromAnyRef(
                                            virtualTableDetail
                                                    .getDatasourceProperties()
                                                    .get(HdfsOptionRule.TIME_FORMAT.key())));
        }

        return super.mergeDatasourceConfig(
                dataSourceInstanceConfig,
                virtualTableDetail,
                dataSourceOption,
                selectTableFields,
                businessMode,
                pluginType,
                connectorConfig);
    }
}
