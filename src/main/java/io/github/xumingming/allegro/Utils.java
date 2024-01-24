package io.github.xumingming.allegro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Files;
import io.github.xumingming.allegro.model.Query;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static io.github.xumingming.beauty.Beauty.detail;
import static io.github.xumingming.beauty.Beauty.draw;
import static io.github.xumingming.beauty.Beauty.drawH2Title;
import static io.github.xumingming.beauty.Column.column;
import static java.lang.String.format;

public class Utils
{
    private Utils()
    {}

    public static void checkState(boolean expression)
    {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean expression, Object errorMessage)
    {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    public static boolean isBlank(String str)
    {
        return str == null || str.trim().length() == 0;
    }

    public static boolean isNotBlank(String str)
    {
        return !isBlank(str);
    }

    public static boolean isEmpty(String str)
    {
        return str == null || str.length() == 0;
    }

    public static void drawSuiteConf(SuiteConf suiteConf)
    {
        drawH2Title(suiteConf.getRunName());
        draw(detail(
                suiteConf,
                Arrays.asList(
                        column("QuerySet", (SuiteConf conf) -> conf.getQuerySet()),
                        column("JdbcUrl", (SuiteConf conf) -> conf.getJdbcUrl("hello")),
                        column("User", (SuiteConf conf) -> conf.getUser()),
                        column("Password", (SuiteConf conf) -> conf.maskedPassword()),
                        column("SessionProperties", (SuiteConf conf) -> conf.getSessionProperties()))));
    }

    public static ObjectMapper getObjectMapper()
    {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.findAndRegisterModules();
        mapper.disable(FAIL_ON_UNKNOWN_PROPERTIES);
        return mapper;
    }

    public static String mask(String str)
    {
        if (str == null) {
            return "<null>";
        }

        if (str.length() == 1) {
            return "*******";
        }

        return str.charAt(0) + "****" + str.charAt(str.length() - 1);
    }

    public static Query parseQuery(String path, boolean normalizeOperatorType)
    {
        try {
            List<String> lines = Files.readLines(new File(path), Charset.defaultCharset());
            String planStr = String.join("\n", lines);
            Query query = PlanParser.parse(PlanParser.Context.of(path, normalizeOperatorType), planStr);
            query.setName(path);
            query.init();
            return query;
        }
        catch (IOException e) {
            throw new RuntimeException("parse query failed!", e);
        }
    }

    public static <K, V> Map<K, V> extractInfoFromQueriesAsMap(String filePath, Function<Query, K> keyExtractor, Function<Query, V> valueExtractor, boolean normalizeOperatorType)
    {
        String[] fileNames = new File(filePath).list();
        Map<K, V> ret = new HashMap<>();

        for (int i = 0; i < fileNames.length; i++) {
            String fileName = fileNames[i];
            if (new File(filePath + "/" + fileName).isDirectory()) {
                continue;
            }

            try {
                Query query = parseQuery(filePath + "/" + fileName, normalizeOperatorType);
                ret.put(keyExtractor.apply(query), valueExtractor.apply(query));
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }

        return ret;
    }

    public static String httpGet(String host, int port, String path)
    {
        try {
            URL url = new URL(format("http://%s:%s%s", host, port, path));
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            con.disconnect();

            return content.toString();
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static String read(String filePath)
    {
        try (BufferedReader newReader = newReader(new File(filePath), StandardCharsets.UTF_8)) {
            List<String> lines = newReader.lines().collect(Collectors.toList());
            String sql = String.join("\n", lines);
            return sql;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static BufferedReader newReader(File file, Charset charset)
            throws FileNotFoundException
    {
        return new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
    }

    public static void append(String filePath, String content)
    {
        try {
            if (new File(filePath).exists()) {
                java.nio.file.Files.write(Paths.get(filePath), content.getBytes(), StandardOpenOption.APPEND);
            }
            else {
                java.nio.file.Files.write(Paths.get(filePath), content.getBytes(), StandardOpenOption.CREATE_NEW);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static List<String> listSubDirs(String filePath)
    {
        return Arrays.stream(new File(filePath).listFiles(x -> x.isDirectory())).map(x -> x.getName()).collect(Collectors.toList());
    }

    public static String formatDateTime(Date date)
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return format.format(date);
    }

    public static String speedup(double value)
    {
        return String.format("%.2fx", value);
    }
}
