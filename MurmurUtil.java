package KafkaREST;

import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.codec.digest.MurmurHash3;

import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;

import java.util.HashMap;

public class MurmurUtil {

    public static int murmur2(final byte[] data) {
        int length = data.length;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;

        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;

        for (int i = 0; i < length4; i++) {
            final int i4 = i * 4;
            int k = (data[i4 + 0] & 0xff) + ((data[i4 + 1] & 0xff) << 8) + ((data[i4 + 2] & 0xff) << 16) + ((data[i4 + 3] & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        // Handle the last few bytes of the input array
        switch (length % 4) {
            case 3:
                h ^= (data[(length & ~3) + 2] & 0xff) << 16;
            case 2:
                h ^= (data[(length & ~3) + 1] & 0xff) << 8;
            case 1:
                h ^= data[length & ~3] & 0xff;
                h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;

        return h;
    }

    public static int toPositive(int number) {
        return number & 0x7fffffff;
    }

    public static int realHash(String key){
        return Math.abs(key.hashCode()%3);
    }



    public static void main(String[] args) throws Exception {
        //66778899-2
        //14253678
        //142536789-2
        // String key = "123465789";
        HashMap<String,String> map = new HashMap<String, String>(){{
            put("14253678","2");
            put("12345678","1");
            put("87654321","1");
            put("13572468","2");
            put("36925814","2");
            put("74185296","0");
            put("15935725","1");
            put("78523912","0");
            put("33669988","0");
        }};

        int count = 0;

        // count = 0;
        // for(String key:map.keySet()){
        //     long h = Math.abs(key.hashCode());
        //     if(Long.parseLong(map.get(key)) == h%3){
        //         count++;
        //     }
        // }
        // System.out.println("hashCode :"+count);
        //
        // count = 0;
        // for(String key:map.keySet()){
        //     long h232 = Math.abs(MurmurHash2.hash32(key.getBytes(),key.getBytes().length));
        //     if(Long.parseLong(map.get(key)) == h232%3){
        //         count++;
        //     }
        // }
        // System.out.println("MurmurHash2.hash32 :"+count);
        //
        // count = 0;
        // for(String key:map.keySet()){
        //     long h264 = Math.abs(MurmurHash2.hash64(key.getBytes(),key.getBytes().length));
        //     // System.out.println(h264%3);
        //     if(Long.parseLong(map.get(key)) == h264%3){
        //         count++;
        //     }
        // }
        // System.out.println("MurmurHash2.hash64 :"+count);
        //
        // count = 0;
        // for(String key:map.keySet()){
        //     long h332 = Math.abs(MurmurHash3.hash32(key.getBytes(),key.getBytes().length));
        //     if(Long.parseLong(map.get(key)) == h332%3){
        //         count++;
        //     }
        // }
        // System.out.println("MurmurHash3.hash32 :"+count);
        //
        // count = 0;
        // for(String key:map.keySet()){
        //     long h364 = Math.abs(MurmurHash3.hash64(key.getBytes()));
        //     if(Long.parseLong(map.get(key)) == h364%3){
        //         count++;
        //     }
        // }
        // System.out.println("MurmurHash3.hash64 :"+count);

        count = 0;
        IntegerSerializer integerSerializer = new IntegerSerializer();
        ShortSerializer shortSerializer = new ShortSerializer();
        LongSerializer longSerializer = new LongSerializer();
        StringSerializer stringSerializer = new StringSerializer();
        BytesSerializer bytesSerializer = new BytesSerializer();
        for(String key:map.keySet()){
            long hm1 = toPositive(murmur2(
                    integerSerializer.serialize("rest_test2",Integer.parseInt(key))));
            if(Long.parseLong(map.get(key)) == hm1%3){
                count++;
                System.out.println("integerSerializer"+key);
                
            }
            try {
                long hm2 = toPositive(murmur2(
                        shortSerializer.serialize("rest_test2", Short.parseShort(key))));
                if (Long.parseLong(map.get(key)) == hm2 % 3) {
                    count++;
                    System.out.println("shortSerializer"+key);

                }
            }catch (Exception e){}
            long hm3 = toPositive(murmur2(
                    longSerializer.serialize("rest_test2",Long.parseLong(key))));
            if(Long.parseLong(map.get(key)) == hm3%3){
                count++;
                System.out.println("longSerializer"+key);
                
            }
            long hm4 = toPositive(murmur2(
                    stringSerializer.serialize("rest_test2",key)));
            if(Long.parseLong(map.get(key)) == hm4%3){
                count++;
                System.out.println("stringSerializer"+key);
                
            }
            long hm5 = toPositive(murmur2(
                    bytesSerializer.serialize("rest_test2", Bytes.wrap(key.getBytes()))));
            if(Long.parseLong(map.get(key)) == hm5%3){
                count++;
                System.out.println("bytesSerializer"+key);
                
            }
        }
        System.out.println("murmur2 :"+count);

        // count = 0;
        // for(String key:map.keySet()){
        //     long rh = realHash(key);
        //     if(Long.parseLong(map.get(key)) == rh){
        //         count++;
        //     }
        // }
        // System.out.println("realHash :"+count);


    }
}
