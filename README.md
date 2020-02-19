# 使用方式
```java

   StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);
   String[] outputFieldNames = new String[]{"age", "name", "nick", "result"};
        TypeInformation<?>[] outputTypes = new TypeInformation[]{
                Types.LONG(),
                Types.STRING(),
                Types.STRING(),
                Types.LONG()};
 OpenTSDBTableSink tsdbTableSink = new OpenTSDBTableSink(outputFieldNames, outputTypes, jobConfig.getOpenTSDBHost(), "xxx_metrics");
 bsTableEnv.registerTableSink("opentsdb_output", tsdbTableSink);

```