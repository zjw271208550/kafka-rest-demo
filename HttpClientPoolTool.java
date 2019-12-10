package KafkaREST;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.*;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.LayeredConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * HttpClient连接池
 * 说明：
 * 1.http连接池不是万能的,过多的长连接会占用服务器资源,导致其他服务受阻
 * 2.http连接池只适用于请求是经常访问同一主机(或同一个接口)的情况下
 * 3.并发数不高的情况下资源利用率低下
 * 优点：
 * 1.复用 http连接,省去了tcp的3次握手和4次挥手的时间,极大降低请求响应的时间
 * 2.自动管理 tcp连接,不用人为地释放/创建连接
 */
public class HttpClientPoolTool {
    private static Logger logger = LoggerFactory.getLogger(HttpClientPoolTool.class);

    // 设置连接建立的超时时间为10s
    private static final int CONNECT_TIMEOUT = 6000;
    private static final int SOCKET_TIMEOUT = 10000;
    private static final int HTTP_IDEL_TIMEOUT = 30000;
    private static final int THREAD_FIRST_DELAY = 0;
    private static final int THREAD_MONITOR_INTERVAL = 30000;
    private static final int HTTP_RETRY_TIME = 3;
    // 最大连接数
    private static final int MAX_CONN = 2048;
    private static final int MAX_PRE_ROUTE = 1024;
    private static final int MAX_ROUTE = 2048;
    // 发送请求的客户端单例
    private static CloseableHttpClient httpClient;
    //连接池管理类
    private static PoolingHttpClientConnectionManager manager;
    private static ScheduledExecutorService monitorExecutor;


    /**
     * 获取一个 HttpClient
     * 多线程下多个线程同时调用getHttpClient
     * 容易导致重复创建httpClient对象的问题,所以加上了同步锁
     * @param path 路径
     * @return CloseableHttpClient
     */
    public synchronized static CloseableHttpClient getHttpClient(String path){
        // 初始化 CloseableHttpClient
        if (httpClient == null){
            String host = "";
            int port = 0;
            try{
                URL url = new URL(path);
                host = url.getHost();
                port = url.getPort();
            }catch (Exception e){
                logger.error("HTTP Url 格式格式异常：{}",path,e);
            }
            httpClient = createHttpClient(host, port);
            //开启监控线程,对异常和空闲线程进行关闭
            // https://www.yeetrack.com/?p=782 搜索 closeExpiredConnections()方法
            monitorExecutor = Executors.newScheduledThreadPool(1);
            monitorExecutor.scheduleWithFixedDelay(
                    new TimerTask() {
                        @Override
                        public void run() {
                            //关闭过期连接
                            manager.closeExpiredConnections();
                            //关闭5s空闲的连接
                            manager.closeIdleConnections(HTTP_IDEL_TIMEOUT, TimeUnit.MILLISECONDS);
                            logger.info("关闭过期或不活动{}ms的连接",HTTP_IDEL_TIMEOUT);
                        }
                    },
                    THREAD_FIRST_DELAY,//??
                    THREAD_MONITOR_INTERVAL,
                    TimeUnit.MILLISECONDS);

        }
        return httpClient;
    }


    /**
     * 根据host和port构建httpclient实例
     * @param host 要访问的域名
     * @param port 要访问的端口
     * @return CloseableHttpClient
     */
    private static CloseableHttpClient createHttpClient(String host, int port){
        ConnectionSocketFactory plainSocketFactory = PlainConnectionSocketFactory.getSocketFactory();
        LayeredConnectionSocketFactory sslSocketFactory = SSLConnectionSocketFactory.getSocketFactory();
        Registry<ConnectionSocketFactory> registry = RegistryBuilder
                .<ConnectionSocketFactory> create()
                .register("http", plainSocketFactory)
                .register("https", sslSocketFactory).build();

        manager = new PoolingHttpClientConnectionManager(registry);

        // 设置连接参数
        // 最大连接数
        manager.setMaxTotal(MAX_CONN);
        // 路由最大连接数
        manager.setDefaultMaxPerRoute(MAX_PRE_ROUTE);

        HttpHost httpHost = new HttpHost(host, port);
        manager.setMaxPerRoute(new HttpRoute(httpHost), MAX_ROUTE);

        //请求失败时,进行请求重试
        HttpRequestRetryHandler handler = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException e, int i, HttpContext httpContext) {
                if (i > HTTP_RETRY_TIME){
                    logger.error("重试超过{}次，请求失败",HTTP_RETRY_TIME);
                    return false;
                }
                if (e instanceof NoHttpResponseException){
                    logger.error("服务器无响应，重试",e);
                    return true;
                }
                if (e instanceof SSLHandshakeException){
                    logger.error("SSL 握手异常，请求失败",e);
                    return false;
                }
                if (e instanceof InterruptedIOException){
                    logger.error("请求超时，请求失败",e);
                    return false;
                }
                if (e instanceof UnknownHostException){
                    logger.error("未知的服务器，HOST：{}，POST：{}",host,port,e);
                    return false;
                }
                if (e instanceof ConnectTimeoutException){
                    logger.error("连接超时，请求失败",e);
                    return false;
                }
                if (e instanceof SSLException){
                    logger.error("SSL 异常，请求失败",e);
                    return false;
                }

                HttpClientContext context = HttpClientContext.adapt(httpContext);
                HttpRequest request = context.getRequest();
                if (!(request instanceof HttpEntityEnclosingRequest)){
                    //如果请求不是关闭连接的请求
                    return true;
                }
                return false;
            }
        };

        CloseableHttpClient client = HttpClients
                .custom()
                .setConnectionManager(manager)
                .setRetryHandler(handler)
                .build();
        return client;
    }


    /**
     * 关闭连接池
     */
    public static void closeConnectionPool(){
        try {
            httpClient.close();
            manager.close();
            monitorExecutor.shutdown();
        } catch (IOException e) {
            logger.error("Http 连接池关闭异常",e);
        }
    }



    /**
     * 配置请求
     * @param request 一个请求
     * @param contentType contentType
     * @param accept accept
     * @param entity StringEntity
     */
    private static void setConfig(HttpRequestBase request,String contentType,String accept,StringEntity entity){
        RequestConfig requestConfig = RequestConfig
                .custom()
                .setConnectionRequestTimeout(CONNECT_TIMEOUT)
                .setConnectTimeout(CONNECT_TIMEOUT)
                .setSocketTimeout(SOCKET_TIMEOUT)
                .build();
        request.setConfig(requestConfig);
        if(!StringUtils.isBlank(contentType)) {
            request.setHeader("Content-Type",contentType);
        }
        if(!StringUtils.isBlank(accept)) {
            request.setHeader("Accept",accept);
        }
        if(null != entity && request instanceof HttpPost){
            ((HttpPost) request).setEntity(entity);
        }
    }


    /**
     * 配置 stringEntity
     * @param json 数据 json
     * @param encode 编码格式
     * @param contentType contentType
     * @return StringEntity
     */
    private static StringEntity setStringEntity(String json,String encode,String contentType){
        StringEntity stringEntity = new StringEntity(json,encode);
        if(!StringUtils.isBlank(contentType)) {
            stringEntity.setContentType(contentType);
        }
        return stringEntity;
    }


    /**
     * 发一个 Restful 的 POST 请求
     * @param path 请求路径
     * @param contentType contentType
     * @param accept accept
     * @param json 数据 json
     * @param encode 编码格式
     * @return response json
     */
    public static String postRest(String path,String contentType,String accept,String json,String encode){
        HttpPost post = new HttpPost(path);
        StringEntity stringEntity = setStringEntity(json, encode, contentType);
        setConfig(post,contentType,accept,stringEntity);

        CloseableHttpResponse response = null;
        try {
            response = getHttpClient(path)
                    .execute(post);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return EntityUtils.toString(response.getEntity());
            }else {
                return "{\"failed_message\":\""+response.getStatusLine().getReasonPhrase()+"\"}";
            }
        }catch (IOException e){
            logger.error("POST 异常",e);
            return "{\"error_message\":\""+e.getMessage()+"\"}";
        }finally {
            try{
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                logger.error("连接关闭异常",e);
            }

        }
    }

    /**
     * 发一个 Restful 的 GET 请求
     * @param path 请求路径
     * @param contentType contentType
     * @param accept accept
     * @return response json
     */
    public static String getRest(String path,String contentType,String accept){
        HttpGet get = new HttpGet(path);
        setConfig(get,contentType,accept,null);

        CloseableHttpResponse response = null;
        try {
            response = getHttpClient(path)
                    .execute(get, HttpClientContext.create());
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                return EntityUtils.toString(response.getEntity());
            }else {
                return "{\"failed_message\":\""+response.getStatusLine().getReasonPhrase()+"\"}";
            }
        }catch (IOException e){
            logger.error("POST 异常",e);
            return "{\"error_message\":\""+e.getMessage()+"\"}";
        }
    }
}
