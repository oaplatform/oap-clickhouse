package oap.clickhouse;

import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import oap.clickhouse.migration.Database;
import oap.util.Dates;
import oap.util.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.utils.URIBuilder;
import org.joda.time.DateTimeUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static oap.util.Dates.m;
import static oap.util.Dates.s;

/**
 * Created by igor.petrenko on 28.02.2018.
 */
@Slf4j
public class DefaultClickHouseClient implements ClickHouseClient {
    private static final String PUT = "INSERT INTO ${TABLE} ${FIELDS} FORMAT ${FORMAT}";
    private static final String DROP_PARTITION = "ALTER TABLE ${TABLE} DROP PARTITION '${PARTITION}'";
    private static final String DROP_TABLE = "DROP TABLE IF EXISTS ${DATABASE}.${TABLE}";
    private static final String DROP_DATABASE = "DROP DATABASE IF EXISTS ${DATABASE}";
    private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS ${DATABASE}";
    public final String host;
    public final int port;
    private final String database;
    private final HttpClient client;
    public int maxQuerySize = -1;
    public int max_ast_elements = -1;
    public int max_expanded_ast_elements = -1;
    public String charsetName = "UTF-8";
    public int chunkSize = 1024 * 1024;
    public long timeout;

    private String user = null;

    public DefaultClickHouseClient(String host, int port, String database) {
        this(host, port, database, s(60), m(5));
    }

    public DefaultClickHouseClient(String host, int port, String database, long connectTimeout, long timeout) {
        this(host, port, database, -1, -1, -1, "UTF-8",
                connectTimeout, timeout);
    }

    public DefaultClickHouseClient(String host, int port, String database, int maxQuerySize,
                                   int max_ast_elements, int max_expanded_ast_elements, String charsetName,
                                   long connectTimeout, long timeout) {
        this(host, port, database, maxQuerySize, max_ast_elements, max_expanded_ast_elements, charsetName,
                timeout,
                HttpClient.newBuilder().connectTimeout(Duration.ofMillis(connectTimeout)).build());
    }

    public DefaultClickHouseClient(String host, int port, String database, int maxQuerySize,
                                   int max_ast_elements, int max_expanded_ast_elements, String charsetName,
                                   long timeout,
                                   HttpClient client) {
        this.host = host;
        this.database = database;
        this.port = port;
        this.timeout = timeout;
        this.maxQuerySize = maxQuerySize;
        this.max_ast_elements = max_ast_elements;
        this.max_expanded_ast_elements = max_expanded_ast_elements;
        this.charsetName = charsetName;
        this.client = client;
    }

    @Override
    public long getTimeout() {
        return timeout;
    }

    @Override
    public List<String> getLines(String query, boolean useDatabase, long timeout) throws ClickHouseException {
        var lines = new ArrayList<String>();
        get(query, lines::add, useDatabase, timeout);

        return lines;
    }

    @Override
    public OutputStream put(String table, DataFormat format, long timeout) {
        log.trace("put OutputStream into {}", table);
        return execute(new Query(getSubstitute(table, PUT, (v) ->
                switch (v) {
                    case "FIELDS" -> "";
                    case "FORMAT" -> format.name();
                    default -> null;
                }), true), true, timeout).getOutputStream();
    }

    @Override
    public void deleteRows(String table, String where, boolean async, long timeout) {
        log.trace("delete from {} WHERE {}", table, where);

        execute("ALTER TABLE " + table + " DELETE WHERE " + where, true, timeout);

        if (!async) waitMutation(where);
    }

    private void waitMutation(String where) {
        try {
            where = StringUtils.replace(where, "'", "\\'");
            where = where.replaceAll("  +", " ");

            var time = DateTimeUtils.currentTimeMillis();
            List<String> lines;
            do {
                if (DateTimeUtils.currentTimeMillis() - time > timeout)
                    throw new ClickHouseException(new TimeoutException());
                lines = getLines("SELECT is_done FROM system.mutations where is_done = 0 AND command = 'DELETE WHERE " + where + "'");
                if (lines.isEmpty()) return;
                Thread.sleep(500);
            } while (true);
        } catch (InterruptedException e) {
            throw new ClickHouseException(e);
        }

    }

    @Override
    public ClickHouseProcess putAsync(String table, DataFormat format, long timeout) {
        return executeAsync(getSubstitute(table, PUT, (v) ->
                switch (v) {
                    case "FIELDS" -> "";
                    case "FORMAT" -> format.name();
                    default -> null;
                }), true, timeout);
    }

    @Override
    public ClickHouseProcess putAsync(String table, DataFormat format, Collection<String> fields, long timeout) {
        return executeAsync(getSubstitute(table, PUT, (v) ->
                switch (v) {
                    case "FIELDS" -> Strings.join(",", fields, "(", ")");
                    case "FORMAT" -> format.name();
                    default -> null;
                }), true, timeout);
    }

    @SneakyThrows
    @Override
    public void execute(String sql, boolean useDatabase, long timeout) {
        log.trace("Executing SQL \"{}\"...", sql);
        try (var stream = executeAsync(getSubstitute(null, sql, null), useDatabase, timeout).toStream();
             var ignored = stream.getInputStream()) {
        }
        log.trace("Executing SQL \"{}\"... Done", sql);
    }

    @Override
    public void createDatabase(long timeout) {
        log.debug("create database {}...", database);
        execute(getSubstitute(null, CREATE_DATABASE, null), false, timeout);
    }

    @Override
    public void dropDatabase(long timeout) {
        log.debug("drop database {}...", database);
        execute(getSubstitute(null, DROP_DATABASE, null), false, timeout);
    }

    @Override
    public void dropTable(String table, long timeout) {
        log.debug("drop table={}, database={}...", table, database);
        execute(getSubstitute(table, DROP_TABLE, null), true, timeout);
    }

    @Override
    public int get(String query, Consumer<String> consumer, boolean useDatabase, long timeout) throws ClickHouseException {
        var cmd = getSubstitute(null, query, null);

        log.trace("execute {}", cmd);

        try (var stream = execute(new Query(cmd, false),
                useDatabase ? database : null, timeout)) {

            String line;
            int count = 0;
            try (BufferedReader br = new BufferedReader(new java.io.InputStreamReader(stream.getInputStream(), charsetName))) {
                while ((line = br.readLine()) != null) {
                    consumer.accept(line);
                    count++;
                }
            }
            log.trace("Finished executing GET with {} lines", count);

            return count;
        } catch (IOException e) {
            throw new ClickHouseException(e);
        }
    }

    @Override
    public Database getDatabase() {
        return new Database(database, this);
    }

    @Override
    public ClickHouseClient useDatabase(String database) {
        return new DefaultClickHouseClient(host, port, database, maxQuerySize, max_ast_elements,
                max_expanded_ast_elements, charsetName, timeout, client);
    }

    @Override
    public ClickHouseClient withUser(String user) {
        return new DefaultClickHouseClient(host, port, database, maxQuerySize, max_ast_elements,
                max_expanded_ast_elements, charsetName, timeout, client);
    }

    private String getSubstitute(String table, String query, Function<String, String> ifElse) {
        return Strings.substitute(query, f ->
                switch (f) {
                    case "DATABASE" -> database;
                    case "TABLE" -> table;
                    case "PORT" -> port;
                    default -> ifElse != null ? ifElse.apply(f) : null;
                });
    }

    @SneakyThrows
    private ClickhouseStream execute(Query query, boolean useDatabase, long timeout) {
        return execute(query, useDatabase ? database : null, timeout);
    }

    @SneakyThrows
    private ClickhouseStream execute(Query query, String database, long timeout) throws ClickHouseException {
        var process = newProcess(query, database, timeout);
        return process.toStream();
    }

    @Override
    public ClickHouseProcess executeAsync(String query, boolean useDatabase, long timeout) {
        return newProcess(new Query(query, false), useDatabase, timeout);
    }

    private ClickHouseProcess newProcess(Query query, String database, long timeout) {
        return new ClickHouseProcessHttp(query, database, user, timeout);
    }

    private ClickHouseProcess newProcess(Query query, boolean useDatabase, long timeout) {
        return new ClickHouseProcessHttp(query, useDatabase, user, timeout);
    }

    @ToString
    @EqualsAndHashCode
    public static class Query {
        public final String query;
        public final boolean commandLine;

        public Query(String query, boolean commandLine) {
            this.query = query;
            this.commandLine = commandLine;
        }
    }

    public static class ProcessStreamReader implements Consumer<InputStream> {
        private final StringBuilder stringBuilder = new StringBuilder();
        private final String charsetName;

        public ProcessStreamReader(String charsetName) {
            this.charsetName = charsetName;
        }

        @Override
        @SneakyThrows
        public void accept(InputStream inputStream) {
            String line;
            var br = new BufferedReader(new InputStreamReader(inputStream, charsetName));
            while ((line = br.readLine()) != null) {
                stringBuilder.append(line);
            }
        }

        @Override
        public String toString() {
            return stringBuilder.toString();
        }
    }

    public class ClickHouseProcessHttp implements ClickHouseProcess {
        private final Query query;
        private final String database;
        private final String user;
        private final long timeout;

        public ClickHouseProcessHttp(Query query, boolean useDatabase, String user, long timeout) {
            this(query, useDatabase ? DefaultClickHouseClient.this.database : null, user, timeout);
        }

        public ClickHouseProcessHttp(Query query, String database, String user, long timeout) {
            this.query = query;
            this.database = database;
            this.user = user;
            this.timeout = timeout;
        }

        @Override
        public ClickhouseStream toStream() throws ClickHouseException {
            try {
                var uriBuilder = new URIBuilder("http://" + host + ":" + port);
                if (this.database != null) uriBuilder.addParameter("database", this.database);
                if (maxQuerySize > 0) uriBuilder.addParameter("max_query_size", String.valueOf(maxQuerySize));
                if (max_ast_elements > 0)
                    uriBuilder.addParameter("max_ast_elements", String.valueOf(max_ast_elements));
                if (max_expanded_ast_elements > 0)
                    uriBuilder.addParameter("max_expanded_ast_elements", String.valueOf(max_expanded_ast_elements));
                if (user != null) uriBuilder.addParameter("user", user);
                var uri = uriBuilder.build();

                log.trace("clickhouse uri = {}, chunk size = {}, connection timeout = {}",
                        uri, FileUtils.byteCountToDisplaySize(chunkSize), Dates.durationToString(timeout));

                var con = uri.toURL().openConnection();
                var http = (HttpURLConnection) con;
                http.setRequestMethod("POST");
                http.setDoOutput(true);
                http.setChunkedStreamingMode(chunkSize);
                http.connect();
                http.setConnectTimeout((int) timeout);

                var os = http.getOutputStream();

                os.write(query.query.getBytes());
                os.write('\n');

                return new ClickhouseStream(os, http);

            } catch (URISyntaxException | IOException e) {
                throw new ClickHouseException(e);
            }
        }
    }
}
