package top.huster.sink;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.SerializableObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * @Author : rentianbing
 * @Date : 2020/2/17 4:50 下午
 *
 ***/
public class OpenTSDBSinkFunction<IN>  extends RichSinkFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(OpenTSDBSinkFunction.class);
    private static final long serialVersionUID = -3415521837204305936L;
    private CloseableHttpClient httpClient;
    private RequestConfig requestConfig;
    private String host;
    private final SerializableObject lock = new SerializableObject();
    private final OpenTsDBSerializationSchema serializationSchema;


    public OpenTSDBSinkFunction(String host, OpenTsDBSerializationSchema serializationSchema) {
        this.host = host;
        this.serializationSchema = serializationSchema;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        synchronized (lock) {
            createConnection();
        }
    }

    @Override
    public void close() throws Exception {
        // clean up in locked scope, so there is no concurrent change to the stream and client
        synchronized (lock) {
            httpClient.close();
        }
    }

    public void invoke(IN value, Context context) throws Exception {
        HttpPost httpPost = new HttpPost(host + "/api/put");
        StringEntity requestEntity = new StringEntity(serializationSchema.toJson((Row) value), "utf-8");
        requestEntity.setContentEncoding("UTF-8");
        httpPost.setHeader("Content-type", "application/json");
        httpPost.setEntity(requestEntity);
        CloseableHttpResponse response2 = httpClient.execute(httpPost);
        if (response2.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
            //logger
            try {
                HttpEntity entity2 = response2.getEntity();
                LOG.error("put data error {}", EntityUtils.toString(entity2));
            } catch (Exception e) {
                response2.close();
            } finally {
                response2.close();
            }
        }
    }
        private void createConnection() {
        this.httpClient = HttpClients.createDefault();
        this.requestConfig = RequestConfig.custom()
                .setSocketTimeout(1000)
                .setConnectTimeout(3000)
                .setConnectionRequestTimeout(3000)
                .build();
    }

    public CloseableHttpClient getHttpClient() {
        return httpClient;
    }

    public void setHttpClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    public RequestConfig getRequestConfig() {
        return requestConfig;
    }

    public void setRequestConfig(RequestConfig requestConfig) {
        this.requestConfig = requestConfig;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
