/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/** Whether a node serves default-mode indices, beside neural search or not. */
public class InfinoDefaultModeTests extends OpenSearchTestCase {

    /** A node home, with neural search installed in its plugins when {@code neural}. */
    private Path home(boolean neural) throws IOException {
        Path home = createTempDir();
        Files.createDirectories(home.resolve("plugins"));
        if (neural) {
            Files.createDirectories(home.resolve("plugins").resolve(InfinoPlugin.NEURAL_SEARCH_PLUGIN));
        }
        return home;
    }

    private static Settings settings(Path home, Boolean defaultMode) {
        Settings.Builder b = Settings.builder().put("path.home", home.toString());
        if (defaultMode != null) {
            b.put(InfinoPlugin.DEFAULT_MODE_ENABLED.getKey(), defaultMode);
        }
        return b.build();
    }

    public void testWithoutNeuralSearchTheNodeServesDefaultMode() throws IOException {
        Path home = home(false);
        assertFalse(InfinoPlugin.neuralSearchInstalled(settings(home, null)));
        assertTrue(InfinoPlugin.servesDefaultMode(settings(home, null)));
        assertFalse(InfinoPlugin.servesDefaultMode(settings(home, false)));
    }

    /** The standard OpenSearch bundle ships neural search: the node starts, in proxy mode. */
    public void testBesideNeuralSearchAnUnsetNodeServesProxyMode() throws IOException {
        Path home = home(true);
        assertTrue(InfinoPlugin.neuralSearchInstalled(settings(home, null)));
        assertFalse(InfinoPlugin.servesDefaultMode(settings(home, null)));
        assertFalse(InfinoPlugin.servesDefaultMode(settings(home, false)));
    }

    /** Asked for default mode beside neural search, the node refuses to start, naming the way out. */
    public void testDefaultModeAskedForBesideNeuralSearchIsRefused() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InfinoPlugin.servesDefaultMode(settings(home(true), true))
        );
        assertTrue(e.getMessage(), e.getMessage().contains("bin/opensearch-plugin remove " + InfinoPlugin.NEURAL_SEARCH_PLUGIN));
        assertTrue(e.getMessage(), e.getMessage().contains(InfinoPlugin.DEFAULT_MODE_ENABLED.getKey() + ": false"));
    }

    public void testWithoutAHomeNothingIsInstalled() {
        assertFalse(InfinoPlugin.neuralSearchInstalled(Settings.EMPTY));
        assertTrue(InfinoPlugin.servesDefaultMode(Settings.EMPTY));
    }
}
