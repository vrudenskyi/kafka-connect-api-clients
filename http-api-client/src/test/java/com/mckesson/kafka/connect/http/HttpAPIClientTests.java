package com.mckesson.kafka.connect.http;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Assert;
import org.junit.Test;

import okhttp3.MediaType;
import okhttp3.Request;

public class HttpAPIClientTests {

  @Test
  public void defaultsTest() throws Throwable {

    String serverUri = "http://localhost";
    String endpoint = "/1234/";
    //String method = "";

    HttpAPIClient client = new JsonGetAPIClient();

    Map<String, String> clientConfig = new HashMap<>();
    try {
      client.configure(clientConfig);
      Assert.fail();
    } catch (ConfigException e) {
    } catch (Exception e) {
      Assert.fail();
    }

    //minimal required config 
    clientConfig.put(HttpAPIClient.SERVER_URI_CONFIG, serverUri);
    clientConfig.put(HttpAPIClient.ENDPPOINT_CONFIG, endpoint);
    client.configure(clientConfig);

    Map<String, Object> partition = client.partitions().get(0);
    Assert.assertTrue(partition.containsKey(HttpAPIClient.PARTITION_URL_KEY));
    Assert.assertEquals(serverUri+endpoint, partition.get(HttpAPIClient.PARTITION_URL_KEY));
    
    Assert.assertTrue(partition.containsKey(HttpAPIClient.PARTITION_METHOD_KEY));
    Assert.assertEquals("GET", partition.get(HttpAPIClient.PARTITION_METHOD_KEY));
    
    Map<String, Object> offset = client.initialOffset(partition);
    Request.Builder builder = client.getRequestBuilder(partition, offset, 0);
    Request r = builder.build();
    Assert.assertEquals("GET", r.method());
  }
  
  
  @Test
  public void postTest() throws Throwable {

    String serverUri = "http://localhost";
    String endpoint = "/1234/";
    String method = "POST";
    String contentType = "application/json";
    String payload = "{}";

    HttpAPIClient client = new JsonGetAPIClient();
    Map<String, String> clientConfig = new HashMap<>();

    //minimal required config 
    clientConfig.put(HttpAPIClient.SERVER_URI_CONFIG, serverUri);
    clientConfig.put(HttpAPIClient.ENDPPOINT_CONFIG, endpoint);
    clientConfig.put(HttpAPIClient.METHOD_CONFIG, method);
    clientConfig.put(HttpAPIClient.REQUEST_CONTENT_TYPE_CONFIG, contentType);
    clientConfig.put(HttpAPIClient.REQUEST_PAYLOAD_CONFIG, payload);
    client.configure(clientConfig);

    Map<String, Object> partition = client.partitions().get(0);
    Assert.assertTrue(partition.containsKey(HttpAPIClient.PARTITION_METHOD_KEY));
    Assert.assertEquals(method, partition.get(HttpAPIClient.PARTITION_METHOD_KEY));
    
    Map<String, Object> offset = client.initialOffset(partition);
    Request.Builder builder = client.getRequestBuilder(partition, offset, 0);
    Request r = builder.build();
    Assert.assertEquals(method, r.method());
    Assert.assertEquals(MediaType.parse(contentType).type(), r.body().contentType().type());
    Assert.assertEquals(MediaType.parse(contentType).subtype(), r.body().contentType().subtype());
    Assert.assertEquals(payload.length(), r.body().contentLength());
  }
  
  
  @Test
  public void headersTest() throws Throwable {

    String serverUri = "http://localhost";
    String endpoint = "/1234/";

    HttpAPIClient client = new JsonGetAPIClient();
    Map<String, String> clientConfig = new HashMap<>();
    clientConfig.put(HttpAPIClient.SERVER_URI_CONFIG, serverUri);
    clientConfig.put(HttpAPIClient.ENDPPOINT_CONFIG, endpoint);
    
    clientConfig.put(HttpAPIClient.REQUEST_HEADERS_CONFIG, "h1,Y-TEST");
    clientConfig.put(HttpAPIClient.REQUEST_HEADERS_CONFIG+".h1."+HttpAPIClient.HEADER_NAME_CONFIG, "X-TEST");
    clientConfig.put(HttpAPIClient.REQUEST_HEADERS_CONFIG+".h1."+HttpAPIClient.HEADER_VALUE_CONFIG, "XXXXXX");
    clientConfig.put(HttpAPIClient.REQUEST_HEADERS_CONFIG+".Y-TEST."+HttpAPIClient.HEADER_VALUE_CONFIG, "YYYYYY");
    
    client.configure(clientConfig);

    Map<String, Object> partition = client.partitions().get(0);
    Map<String, Object> offset = client.initialOffset(partition);
    Request.Builder builder = client.getRequestBuilder(partition, offset, 0);
    Request r = builder.build();
    
    Assert.assertFalse(r.headers("X-TEST").isEmpty());
    Assert.assertFalse(r.headers("Y-TEST").isEmpty());
    Assert.assertEquals("XXXXXX", r.headers("X-TEST").get(0));
    Assert.assertEquals("YYYYYY", r.headers("Y-TEST").get(0));
  }
  
  

}
