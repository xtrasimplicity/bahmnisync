package org.openmrs.module.bahmnisyncworker.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.openmrs.api.context.Context;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Route;

public class HttpConnection {
	
	public static OkHttpClient client = createAuthenticatedClient(Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.MASTER_NODE_USER),
			Context.getAdministrationService().getGlobalProperty(BahmniSyncWorkerConstants.MASTER_NODE_PASSWORD));
	
	public static JSONObject doPost(String url, String json) {
		JSONObject jsonObjectResp = null;
		
		try {
						
			MediaType JSON = MediaType.parse("application/json; charset=utf-8");
			
			okhttp3.RequestBody body = RequestBody.create(JSON, json);
			okhttp3.Request request = new okhttp3.Request.Builder().url(url).post(body).build();
			
			okhttp3.Response response = client.newCall(request).execute();
			
			System.out.println(response.toString());
			String networkResp = response.body().string();
			if (!networkResp.isEmpty()) {
				System.out.println(networkResp);
			}
		}
		catch (Exception ex) {
			String err = String.format("{\"result\":\"false\",\"error\":\"%s\"}", ex.getMessage());
			jsonObjectResp = new JSONObject(err);
		}
		
		return jsonObjectResp;
	}
	
	public static String doGetRequest(String url) throws IOException {
		Request request = new Request.Builder().url(url).build();
		Response response = client.newCall(request).execute();
		return response.body().string();
	}

	private static OkHttpClient createAuthenticatedClient(final String username,
	        final String password) {
	    // build client with authentication information.
	    OkHttpClient httpClient = new OkHttpClient.Builder()
	    		.connectTimeout(5, TimeUnit.MINUTES) // connect timeout
	            .writeTimeout(5, TimeUnit.MINUTES) // write timeout
	            .readTimeout(5, TimeUnit.MINUTES) // read timeout
	    		.authenticator(new Authenticator() {
	        public Request authenticate(Route route, Response response) throws IOException {
	            String credential = Credentials.basic(username, password);
	            if (responseCount(response) >= 3) {
	                return null;
	            }
	            return response.request().newBuilder().header("Authorization", credential).build();
	        }
	    }).build();
	    return httpClient;
	}
	
	private static int responseCount(Response response) {
		int result = 1;
		while ((response = response.priorResponse()) != null) {
			result++;
		}
		return result;
	}
	
}
