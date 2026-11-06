/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.env.Environment;

import java.nio.file.Path;

/**
 * The node's directories, as its environment resolves them. On 2.x the
 * environment names them {@code *File()}, so this class has a copy per major.
 */
final class InfinoNodePaths {

    private InfinoNodePaths() {}

    /** Where the node's plugins are installed. */
    static Path plugins(Environment environment) {
        return environment.pluginsDir();
    }
}
