package com.netbric.s5.conductor.handler;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netbric.s5.conductor.rpc.RestfulReply;
import com.netbric.s5.conductor.rpc.RetCode;
import com.netbric.s5.orm.S5Database;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netbric.s5.conductor.HTTPServer.ContextHandler;
import com.netbric.s5.conductor.HTTPServer.Request;
import com.netbric.s5.conductor.HTTPServer.Response;

import java.io.IOException;
import java.util.HashMap;

public class DebugHandler implements ContextHandler
{
	static final Logger logger = LoggerFactory.getLogger(DebugHandler.class);
	@Override
	public int serve(Request request, Response response) throws IOException {
		response.setContentType("text/json; charset=utf-8");

		String op = request.getParameter("op");
		logger.debug("API called: op={}", op);
		RestfulReply reply;
		try
		{
			if ("sql".equals(op)) {
				String sql = new String(request.getBody().readAllBytes());
				HashMap<String, Object> row1 = S5Database.getInstance().sql(sql).first(HashMap.class);

				reply = new RestfulReply(op);
			}
			else
			{
				reply = new RestfulReply(op, RetCode.INVALID_OP, "Invalid op:" + op);
			}
		}
		catch (Exception ex)
		{
			reply = new RestfulReply(op, RetCode.INVALID_OP, ex.getMessage());
			logger.error("Error processing:" + op, ex);
		}
		GsonBuilder builder = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
				.setPrettyPrinting();
		// .serializeNulls();
		Gson gson = builder.create();
		String reply_str = gson.toJson(reply);
		logger.info("{}", reply_str);
		response.send(200, reply_str);
		return 0;
	}
}
