package io.github.xumingming.allegro.service;

import io.github.xumingming.allegro.AllegroConf;
import io.github.xumingming.allegro.service.impl.DefaultConfigService;

import java.util.List;

public interface ConfigService
{
    static ConfigService create()
    {
        return new DefaultConfigService();
    }

    AllegroConf readAllegroConf();

    /**
     * List queries from the specified querySet.
     *
     * @param querySet
     * @return
     */
    List<String> listQueries(String querySet);

    /**
     * Read query text from the specified filePath.
     *
     * @param filePath
     * @return
     */
    String readQuery(String filePath);
}
