package top.huster.sink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

/***
 * @Author : rentianbing
 * @Date : 2020/2/17 4:32 下午
 *
 ***/
public class OpenTSDBTableSink implements AppendStreamTableSink<Row>{

    private String[] fieldNames;
    private TypeInformation<?>[] fieldTypes;
    private String dbHost;
    private final String metricsName;

    public OpenTSDBTableSink(String[] fieldNames,
                             TypeInformation<?>[] fieldTypes,
                             String dbHost,
                             String metricsName) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.dbHost = dbHost;
        this.metricsName = metricsName;
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(getFieldTypes(), getFieldNames());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        return new OpenTSDBTableSink(fieldNames, fieldTypes, dbHost, metricsName);
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Row> dataStream) {
        OpenTSDBSinkFunction<Row> sinkFunction = new OpenTSDBSinkFunction<>(dbHost,
                new OpenTsDBSerializationSchema(metricsName, fieldNames)
        );
        return dataStream
                .addSink(sinkFunction)
                .setParallelism(dataStream.getParallelism())
                .name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public void setFieldTypes(TypeInformation<?>[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }
}
