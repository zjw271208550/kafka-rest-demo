package KafkaREST;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.BASE64Encoder;

import java.util.HashMap;
import java.util.Map;

import static KafkaREST.HttpClientPoolTool.getRest;
import static KafkaREST.HttpClientPoolTool.postRest;


/**
 * Kadka Rest 调用工具类
 */
public class KafkaRestTool {

    private static String HTTP = "http://";

    /**
     * 创建新的 consumer
     * @param host kafka-rest host
     * @param port kafka-rest port
     * @param consumerGroup consumer 所在用户组，若用户组不存在则自动创建
     * @param consumerName consumer 名字
     * @param contentType consumer 对应的contentType
     * @param encode 编码
     * @return 消费所需的 instance_id和 base_uri
     */
    public static String createConsumer(String host,
                                         int port,
                                         String consumerGroup,
                                         String consumerName,
                                         String contentType,
                                         String encode){
        String URL = HTTP+host+":"+port+"/consumers/"+consumerGroup;
        String JSON = "{\n" +
                "\"id\":\""+ consumerName +"\",\n" +
                "\"format\":\"binary\",\n" +
                "\"auto.offset.reset\":\"smallest\",\n" +
                "\"auto.commit.enable\":\"false\"\n" +
                "}";
        return postRest(URL,contentType,null,JSON,encode);
    }



    /**
     * 发送一批消息到指定分区
     * @param host kafka-rest host
     * @param port kafka-rest port
     * @param topic 指定 topic
     * @param partition 指定分区
     * @param kvs 数据 Map
     * @param contentType 数据对应的contentType
     * @param encode 编码
     * @return 当前插入改变的 offsets信息
     */
    public static String sendMessageAll(String host,
                                 int port,
                                 String topic,
                                 int partition,
                                 HashMap<String,String> kvs,
                                 String contentType,
                                 String encode) {
        BASE64Encoder encoder = new BASE64Encoder();
        String URL = HTTP+host+":"+port+"/topics/"+topic+"/partitions/"+partition;
        String JSON = "{\"records\":[";
        if (null != kvs && !kvs.isEmpty()) {
            for (Map.Entry<String, String> entry : kvs.entrySet()) {
                JSON = JSON
                        + "{\"key\":\""
                        + entry.getKey()
                        + "\",\"value\":\""
                        + encoder.encode(entry.getValue().getBytes())
                        + "\"},";
            }
            JSON = JSON.substring(0, JSON.length() - 1);
        }
        JSON = JSON + "]}";
        return postRest(URL,contentType,null,JSON,encode);
    }



    /**
     * 发送一条消息到指定分区
     * @param host kafka-rest host
     * @param port kafka-rest port
     * @param topic 指定 topic
     * @param partition 指定分区
     * @param key 数据的key
     * @param value 数据的内容
     * @param contentType  数据对应的contentType
     * @param encode 编码
     * @return 当前插入改变的 offsets信息
     */
    public static String sendMessage(String host,
                              int port,
                              String topic,
                              int partition,
                              String key,
                              String value,
                              String contentType,
                              String encode) {
        BASE64Encoder encoder = new BASE64Encoder();
        String URL = HTTP+host+":"+port+"/topics/"+topic+"/partitions/"+partition;
        String JSON = "{\"records\":[";
        JSON = JSON
                        + "{\"key\":\""
                        + key
                        + "\",\"value\":\""
                        + encoder.encode(value.getBytes())
                        + "\"}";

        JSON = JSON + "]}";
        return postRest(URL,contentType,null,JSON,encode);
    }



    /**
     *
     * @param host kafka-rest host
     * @param port kafka-rest port
     * @param topic 指定 topic
     * @param key 数据
     * @param value 数据
     * @param contentType  数据对应的contentType
     * @param encode 编码
     * @return 当前插入改变的 offsets信息
     */
    public static String sendMessage(String host,
                              int port,
                              String topic,
                              String key,
                              String value,
                              String contentType,
                              String encode) {
        BASE64Encoder encoder = new BASE64Encoder();
        String URL = HTTP+host+":"+port+"/topics/"+topic;
        String JSON = "{\"records\":[";
        JSON = JSON
                + "{\"key\":\""
                + key
                + "\",\"value\":\""
                + encoder.encode(value.getBytes())
                + "\"}";

        JSON = JSON + "]}";
        return postRest(URL,contentType,null,JSON,encode);
    }


    /**
     * 消费消息
     * @param host kafka-rest host
     * @param port kafka-rest port
     * @param topic 指定主题
     * @param consumerGroup 指定用户组
     * @param instanceId  指定用户
     * @param accept  数据对应的accept
     * @param contentType  数据对应的contentType
     * @return 消息 JSON
     */
    public static String getMessage(String host,
                             int port,
                             String topic,
                             String consumerGroup,
                             String instanceId,
                             String accept,
                             String contentType) {
        String URL = HTTP+host+":"+port+"/consumers/"+consumerGroup+"/instances/"+instanceId+"/topics/"+topic;
        return getRest(URL,contentType,accept);
    }


    /**
     * 检查请求是否成功
     * @param resultJson 请求返回的
     * @return 成功与否
     */
    public static boolean checkSuccess(String resultJson){
        return resultJson.contains("\"error_message\"") || resultJson.contains("\"failed_message\"");
    }


    //
    // public static void main(String[] args) {
    //     boolean HAS_CREATED = false;
    //
    //     KafkaRestTool helper = new KafkaRestTool("172.16.32.41","8083");
    //
    //     //创建 获取 instanceId
    //     if(!HAS_CREATED){
    //         String relC = helper.createConsumer("rest_test_group","my_consumer1");
    //         System.out.println(relC);
    //     }
    //
    //     //发送
    //     String relS = helper.sendMessageAll("rest_test",0,
    //             new HashMap<String, String>(){{ put("people","我是好人abc");}});
    //     System.out.println(relS);
    //
    //     //消费
    //     String relG = helper.getMessage("rest_test","rest_test_group","my_consumer1");
    //     System.out.println(relG);
    //
    // }
}
