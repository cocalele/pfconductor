package com.netbric.s5.conductor.rpc;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.cluster.ZkHelper;
import com.netbric.s5.conductor.Config;
import com.netbric.s5.conductor.InvalidParamException;
import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class SimpleHttpRpc {
	static final Logger logger = LoggerFactory.getLogger(SimpleHttpRpc.class);
	public static <T extends RestfulReply> T invokeGET(String url, Class<T> replyCls) throws Exception {
		org.eclipse.jetty.client.HttpClient client = new org.eclipse.jetty.client.HttpClient();
		client.start();

		logger.info("Send request:{}", url);
		ContentResponse response = client.newRequest(url)
				.method(org.eclipse.jetty.http.HttpMethod.GET)
				.send();
		logger.info("Get response:{}", response.getContentAsString());
		if(response.getStatus() < 200 || response.getStatus() >= 300)
		{
			throw new IOException(String.format("Failed http GET {}, HTTP status:%d, reason:%s",
					url, response.getStatus(), response.getReason()));
		}
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setPrettyPrinting();
		Gson gson = builder.create();
		T r;
		r = gson.fromJson(new String(response.getContent()), replyCls);
		client.stop();
		if(r.retCode == RetCode.OK)
			logger.info("Succeed http GET {}", url);
		else {
			logger.error("Failed http GET {}", url);
			throw new IOException(String.format("Failed RPC invoke, code:%d, reason:%s", r.retCode, r.reason));
		}
		return r;
	}

	public static <T extends RestfulReply> T invokeStore(String ip, String op, Class<T> replyCls, Object ... args) throws Exception {
		StringBuilder sb = new StringBuilder("http://");
		sb.append(ip).append(":49181/api?op=").append(op);
		if(args.length%2 != 0) {
			throw new InvalidParamException(String.format("Number of http argument:%d not even number, i.e. not name-value pair", args.length));
		}
		for(int i=0;i<args.length;i+=2) {
			sb.append("&").append(args[i]).append("=").append(URLEncoder.encode(args[i+1].toString(), StandardCharsets.UTF_8));
		}
		return invokeGET(sb.toString(), replyCls);
	}

	static String lastLeader;
	public static <T extends RestfulReply> T invokeConductor(Config cfg, String op, Class<T> replyCls, Object ... args) throws Exception {
		if(lastLeader == null) {
			lastLeader = ZkHelper.getLeaderIp(cfg);
		}
		StringBuilder sb = new StringBuilder("http://");
		sb.append(lastLeader).append(":49180/s5c/?op=").append(op);
		if(args.length%2 != 0) {
			throw new InvalidParamException(String.format("Number of http argument:%d not even number, i.e. not name-value pair", args.length));
		}
		for(int i=0;i<args.length;i+=2) {
			sb.append("&").append(args[i]).append("=").append(URLEncoder.encode(args[i+1].toString(), StandardCharsets.UTF_8));
		}
		return invokeGET(sb.toString(), replyCls);
	}
}
