package io.github.xumingming.allegro.service.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.classgraph.ClassGraph;
import io.github.classgraph.ScanResult;
import io.github.xumingming.allegro.AllegroConf;
import io.github.xumingming.allegro.ComparingStringByNumberPart;
import io.github.xumingming.allegro.service.ConfigService;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.github.xumingming.allegro.Constant.CONFIG_FILE_PATH;
import static io.github.xumingming.allegro.Constant.SYSTEM_PREFIX;
import static io.github.xumingming.allegro.Utils.checkState;
import static io.github.xumingming.allegro.Utils.getObjectMapper;
import static io.github.xumingming.allegro.Utils.read;

public class DefaultConfigService
        implements ConfigService
{
    private static String readQueryFromClassPath(String filePath)
    {
        InputStream inputStream = DefaultResultService.class.getClassLoader().getResourceAsStream(filePath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        return bufferedReader.lines().collect(Collectors.joining("\n"));
    }

    private static List<String> listQueriesFromFileSystem(String querySet)
    {
        File dir = new File(querySet);
        String[] fileNames = dir.list();
        List<String> fileNameList = Arrays.stream(fileNames)
                .collect(Collectors.toList());
        return fileNameList;
    }

    private static List<String> listQueriesFromClassPath(String querySet)
    {
        List<String> resourceNames;
        try (ScanResult scanResult = new ClassGraph().acceptPaths(querySet).scan()) {
            resourceNames = scanResult.getAllResources().getPaths();
        }

        return resourceNames
                .stream()
                .map(x -> x.substring(x.lastIndexOf("/") + 1))
                .collect(Collectors.toList());
    }

    public AllegroConf readAllegroConf()
    {
        try {
            ObjectMapper mapper = getObjectMapper();
            checkState(new File(CONFIG_FILE_PATH).exists(), "Config file ~/.allegro/config.yaml missing!");
            AllegroConf allegroConf = mapper.readValue(new File(CONFIG_FILE_PATH), AllegroConf.class);

            return allegroConf;
        }
        catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> listQueries(String querySet)
    {
        List<String> queries;
        if (querySet.startsWith(SYSTEM_PREFIX)) {
            queries = listQueriesFromClassPath(querySet.replaceAll(":", "/"));
        }
        else {
            queries = listQueriesFromFileSystem(querySet);
        }

        return queries.stream()
                .sorted(new ComparingStringByNumberPart<>(Function.identity()))
                .collect(Collectors.toList());
    }

    @Override
    public String readQuery(String filePath)
    {
        if (filePath.startsWith(SYSTEM_PREFIX)) {
            return readQueryFromClassPath(filePath.replaceAll(":", "/"));
        }
        else {
            return read(filePath);
        }
    }
}
