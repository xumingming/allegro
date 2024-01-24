package io.github.xumingming.allegro;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.httpGet;
import static java.lang.String.format;

public class PrestoClient
{
    private static final String SET_SESSION_SQL_TEMPLATE = "SET SESSION %s=%s";
    private final String coordinatorIp;
    private final int coordinatorPort;

    public PrestoClient(String coordinatorIp, int coordinatorPort)
    {
        this.coordinatorIp = coordinatorIp;
        this.coordinatorPort = coordinatorPort;
    }

    public Optional<String> run(String jdbcUrl, String user, String password, Map<String, String> sessionProperties, String sql)
    {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
                Statement stmt = connection.createStatement()) {
            // Set session properties.
            stmt.execute(format(SET_SESSION_SQL_TEMPLATE, "optimize_hash_generation", "false"));
            if (!sessionProperties.isEmpty()) {
                List<String> keys = new ArrayList<>(sessionProperties.size());
                keys.addAll(sessionProperties.keySet());
                Collections.sort(keys);

                for (Map.Entry<String, String> pair : sessionProperties.entrySet()) {
                    stmt.execute(format(SET_SESSION_SQL_TEMPLATE, pair.getKey(), pair.getValue()));
                }
            }

            // Execute the real sql.
            try (ResultSet resultSet = stmt.executeQuery(sql)) {
                while (resultSet.next()) {
                    // Empty, we don't need the result, just need to iterate through it.
                }
            }
            return Optional.empty();
        }
        catch (Exception e) {
            return Optional.of(e.getMessage());
        }
    }

    public String getQueryRawJsonByTraceId(String traceId)
    {
        Optional<String> queryId = getQueryIdByTraceId(traceId);
        checkState(queryId.isPresent(), "Could not find query with traceId: " + traceId);

        return httpGet(coordinatorIp, coordinatorPort, "/v1/query/" + queryId.get());
    }

    private Optional<String> getQueryIdByTraceId(String traceId)
    {
        String allQueriesStr = httpGet(coordinatorIp, coordinatorPort, "/v1/query");
        JSONArray array = JSON.parseArray(allQueriesStr);

        for (int i = 0; i < array.size(); i++) {
            JSONObject query = array.getJSONObject(i);
            JSONObject sessionJson = query.getJSONObject("session");
            String queryTraceId = sessionJson.getString("source");
            String queryType = query.getString("queryType");
            if (!queryType.equals("CONTROL") && traceId.equals(queryTraceId)) {
                return Optional.of(query.getString("queryId"));
            }
        }

        return Optional.empty();
    }
}
