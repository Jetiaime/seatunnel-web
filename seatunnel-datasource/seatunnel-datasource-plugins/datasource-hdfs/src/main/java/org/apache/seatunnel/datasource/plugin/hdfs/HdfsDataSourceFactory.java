package org.apache.seatunnel.datasource.plugin.hdfs;

import org.apache.seatunnel.datasource.plugin.api.DataSourceChannel;
import org.apache.seatunnel.datasource.plugin.api.DataSourceFactory;
import org.apache.seatunnel.datasource.plugin.api.DataSourcePluginInfo;
import org.apache.seatunnel.datasource.plugin.api.DatasourcePluginTypeEnum;

import com.google.auto.service.AutoService;
import com.google.common.collect.Sets;

import java.util.Set;

@AutoService(DataSourceFactory.class)
public class HdfsDataSourceFactory implements DataSourceFactory {

    private static final String PLUGIN_NAME = "Hdfs";

    @Override
    public String factoryIdentifier() {
        return PLUGIN_NAME;
    }

    @Override
    public Set<DataSourcePluginInfo> supportedDataSources() {
        DataSourcePluginInfo hdfsDatasourcePluginInfo =
                DataSourcePluginInfo.builder()
                        .name(PLUGIN_NAME)
                        .type(DatasourcePluginTypeEnum.FILE.getCode())
                        .version("1.0.0")
                        .supportVirtualTables(false)
                        .icon("HdfsFile")
                        .build();

        return Sets.newHashSet(hdfsDatasourcePluginInfo);
    }

    @Override
    public DataSourceChannel createChannel() {
        return new HdfsDatasourceChannel();
    }
}
