package top.huster.domain;

import java.util.Map;

/***
 * @Author : rentianbing
 * @Date : 2020/2/17 6:25 下午
 *
 ***/
public class OpenTSDBDTO {
    private String metric;
    private Long timestamp;
    private String value;
    private Map<String, String> tags;

    public OpenTSDBDTO(String metric, Long timestamp, String value, Map<String, String> tags) {
        this.metric = metric;
        this.timestamp = timestamp;
        this.value = value;
        this.tags = tags;
    }
}
