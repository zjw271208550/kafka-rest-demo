package KafkaREST;

import md5.MD5Util;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static KafkaREST.KafkaRestTool.*;

/**
 * 向 Topic rest_test2(partition_num:3)发送不重复的（md5(random())）数据
 * 请求 JSON 格式为
 * {
 *     "records": [
 *         {
 *             "key": "33669988",
 *             "value": "5oiR5piv5aW95Lq6YWJj"
 *         }
 *     ]
 * }
 *
 * CURL命令如下：
 * curl 172.16.32.41:8083/topics/rest_test2 \
 * -X POST \
 * -H "Content-Type:application/vnd.kafka.binary.v2+json" \
 * -d '{"records": [{"key": "33669988","value": "5oiR5piv5aW95Lq6YWJj"}]}'
 * 返回 Response JSON
 * {"offsets":[{"partition":0,"offset":3008,"error_code":null,"error":null}],"key_schema_id":null,"value_schema_id":null}
 *
 * 测试中使用的 KEY 如下:
 * KEY = 33669988， Postman测试会被分到 Partition 0
 * KEY = 15935725， Postman测试会被分到 Partition 1
 * KEY = 13572468， Postman测试会被分到 Partition 2
 *
 * 每次发送便验证 Response JSON中的分区知否为 Postman测试的分区
 * 如果 是 就计数加一
 * 如果 否 就打印实际分区
 */
public class TestPartition {

    public static int DATA_SIZE = 1000;
    public static String REST_HOST = "172.16.32.41";
    public static int REST_PORT = 8085;
    public static String TOPIC = "rest_test2";
    public static String CONTENT_TYPE = "application/vnd.kafka.binary.v2+json";
    public static String ENCODE = "utf-8";
    public static String KEY_IN_PARTITION_0 = "33669988";
    public static String KEY_IN_PARTITION_1 = "15935725";
    public static String KEY_IN_PARTITION_2 = "13572468";

    public static void main(String[] args) {
        Random random = new Random();

        int count0 = 0;
        int count1 = 0;
        int count2 = 0;

        for(int i=0; i<DATA_SIZE; i++){
            //发送数据
            String response = sendMessage(
                    REST_HOST,
                    REST_PORT,
                    TOPIC,
                    KEY_IN_PARTITION_0,
                    MD5Util.encrypt(
                    String.valueOf(
                            random.nextInt(999999)
                    )),
                    CONTENT_TYPE,
                    ENCODE);
            //验证是否在 Partition 0
            if(0==getPartition(response)){
                count0++;
            }else {
                System.out.println("Need 0 but "+getPartition(response));
            }
        }

        for(int i=0; i<DATA_SIZE/2; i++){
            String response = sendMessage(
                    REST_HOST,
                    REST_PORT,
                    TOPIC,
                    KEY_IN_PARTITION_1,
                    MD5Util.encrypt(
                            String.valueOf(
                                    random.nextInt(999999)
                            )),
                    CONTENT_TYPE,
                    ENCODE);
            if(1==getPartition(response)){
                count1++;
            }else {
                System.out.println(getPartition(response));
            }
        }

        for(int i=0; i<DATA_SIZE/5; i++){
            String response = sendMessage(
                    REST_HOST,
                    REST_PORT,
                    TOPIC,
                    KEY_IN_PARTITION_2,
                    MD5Util.encrypt(
                            String.valueOf(
                                    random.nextInt(999999)
                            )),
                    CONTENT_TYPE,
                    ENCODE);
            if(2==getPartition(response)){
                count2++;
            }else {
                System.out.println(getPartition(response));
            }
        }

        System.out.println(count0);
        System.out.println(count1);
        System.out.println(count2);

        HttpClientPoolTool.closeConnectionPool();
    }

    public static int getPartition(String response){
        try {
            Pattern pattern = Pattern.compile("(?<=(\"partition\":)).*(?=(,\"offset\"))");
            Matcher matcher = pattern.matcher(response);
            if (matcher.find()) {
                return Integer.parseInt(matcher.group(0).trim());
            }
            return -1;
        }catch (Exception e){
            System.out.println(response);
            e.printStackTrace();
        }
        return -1;
    }
}
