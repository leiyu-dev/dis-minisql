package edu.minisql.distributed.protocol;

import java.util.ArrayList;
import java.util.List;

public class SqlResponse {
    public boolean ok;
    public String route;
    public List<ExecuteResponse> responses = new ArrayList<>();
    public String error;

    public static SqlResponse ok(String route, List<ExecuteResponse> responses) {
        SqlResponse response = new SqlResponse();
        response.ok = true;
        response.route = route;
        response.responses = responses;
        return response;
    }

    public static SqlResponse error(String error) {
        SqlResponse response = new SqlResponse();
        response.ok = false;
        response.error = error;
        return response;
    }
}
