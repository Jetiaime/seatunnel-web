package org.apache.seatunnel.datasource.plugin.hdfs;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.datasource.plugin.api.DataSourceChannel;
import org.apache.seatunnel.datasource.plugin.api.DataSourcePluginException;
import org.apache.seatunnel.datasource.plugin.api.model.TableField;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.NonNull;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HdfsDatasourceChannel implements DataSourceChannel {
    @Override
    public OptionRule getDataSourceOptions(@NonNull String pluginName) {
        return HdfsOptionRule.optionRule();
    }

    @Override
    public OptionRule getDatasourceMetadataFieldsByDataSourceName(@NonNull String pluginName) {
        return HdfsOptionRule.metadataRule();
    }

    @Override
    public List<String> getTables(
            @NonNull String pluginName,
            Map<String, String> requestParams,
            String database,
            Map<String, String> options) {
        throw new UnsupportedOperationException("getTables is not supported for Hdfs datasource");
    }

    @Override
    public List<String> getDatabases(
            @NonNull String pluginName, @NonNull Map<String, String> requestParams) {
        throw new UnsupportedOperationException(
                "getDatabases is not supported for Hdfs datasource");
    }

    @Override
    public boolean checkDataSourceConnectivity(
            @NonNull String pluginName, @NonNull Map<String, String> requestParams) {
        Configuration conf = HdfsConfiguration.getConfiguration(requestParams);
        try (FileSystem fs = FileSystem.get(conf)) {
            fs.listStatus(new Path("/"));
            return true;
        } catch (IOException e) {
            throw new DataSourcePluginException(
                    String.format("check hdfs connectivity failed, config is: %s", requestParams),
                    e);
        }
    }

    @Override
    public List<TableField> getTableFields(
            @NonNull String pluginName,
            @NonNull Map<String, String> requestParams,
            @NonNull String database,
            @NonNull String table) {
        throw new UnsupportedOperationException(
                "getTableFields is not supported for Hdfs datasource");
    }

    @Override
    public Map<String, List<TableField>> getTableFields(
            @NonNull String pluginName,
            @NonNull Map<String, String> requestParams,
            @NonNull String database,
            @NonNull List<String> tables) {
        throw new UnsupportedOperationException(
                "getTableFields is not supported for Hdfs datasource");
    }
}
