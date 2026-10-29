/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;

public class InfinoCacheDirTests extends OpenSearchTestCase {

    public void testAnExplicitDirectoryIsTakenAsIs() {
        Path home = createTempDir();
        Settings settings = Settings.builder()
            .put("infino.cache.dir", home.resolve("elsewhere").toString())
            .put("path.home", home.toString())
            .build();
        assertEquals(home.resolve("elsewhere").toString(), InfinoPlugin.cacheDir(settings));
    }

    public void testUnsetItLivesUnderTheFirstDataPath() {
        Path home = createTempDir();
        Settings settings = Settings.builder()
            .put("path.home", home.toString())
            .putList("path.data", home.resolve("a").toString(), home.resolve("b").toString())
            .build();
        assertEquals(home.resolve("a").resolve(InfinoPlugin.DEFAULT_CACHE_SUBDIR).toString(), InfinoPlugin.cacheDir(settings));
    }

    public void testWithoutADataPathItLivesUnderTheHomesData() {
        Path home = createTempDir();
        Settings settings = Settings.builder().put("path.home", home.toString()).build();
        assertEquals(home.resolve("data").resolve(InfinoPlugin.DEFAULT_CACHE_SUBDIR).toString(), InfinoPlugin.cacheDir(settings));
    }

    public void testWithoutAHomeThereIsNone() {
        assertNull(InfinoPlugin.cacheDir(Settings.EMPTY));
    }
}
