package top.huster.sink;

import com.google.gson.Gson;
import org.apache.flink.types.Row;
import top.huster.domain.OpenTSDBDTO;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/***
 * @Author : rentianbing
 * @Date : 2020/2/18 11:44 上午
 *
 ***/
public class OpenTsDBSerializationSchema implements Serializable {

    private static final long serialVersionUID = -609899020461093760L;

    private final String metricsName;

    private final String[] fieldNames;


    public OpenTsDBSerializationSchema(String metricsName, String[] fieldNames) {
        this.metricsName = metricsName;
        this.fieldNames = fieldNames;
    }

    /****
     * 第0位位时间戳
     * 最后一位为计算的数值
     * 其中的为tags
     * @param element
     * @return
     */
    public String toJson(Row element) {
       Map<String, String> tags = new HashMap<String, String>();
       for (int i=1; i < fieldNames.length - 1; i++) {
           tags.put(fieldNames[i], element.getField(i).toString());
       }
       Long timestamp = (Long)element.getField(0);
       String value =  element.getField(element.getArity() -1).toString();
       OpenTSDBDTO openTSDBDTO = new OpenTSDBDTO(metricsName, timestamp, value, tags);
       return new Gson().toJson(openTSDBDTO);
    }
}
