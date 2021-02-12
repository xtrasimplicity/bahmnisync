package com.ihsinformatics.bahmnisyncclient.connection;

import java.io.IOException;
import java.util.ArrayList;

import org.json.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class HTTPConnection {
	
	 static OkHttpClient client = new OkHttpClient();
	
	public static JSONObject doPost(String url, String json) {
        JSONObject jsonObjectResp = null;

        try {

            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
           
            okhttp3.RequestBody body = RequestBody.create(JSON, json);
            okhttp3.Request request = new okhttp3.Request.Builder()
                    .url(url)
                    .post(body)
                    .build();

            okhttp3.Response response = client.newCall(request).execute();

            System.out.println(response.toString());
            String networkResp = response.body().string();
            if (!networkResp.isEmpty()) {
                System.out.println(networkResp);
            }
        } catch (Exception ex) {
            String err = String.format("{\"result\":\"false\",\"error\":\"%s\"}", ex.getMessage());
            jsonObjectResp = new JSONObject(err);
        }

        return jsonObjectResp;
    }
	
	public static String doGetRequest(String url) throws IOException {
        Request request = new Request.Builder()
            .url(url)
            .build();

        Response response = client.newCall(request).execute();
        return response.body().string();
      }

}
