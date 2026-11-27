/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.index.IndexCreationValidator;
import org.opensearch.plugins.Plugin;

import java.util.Collection;
import java.util.List;

/**
 * The plugin's base on OpenSearch 3.7 and later. Those releases let a plugin
 * check an index's mapping as it is created, after its templates are
 * applied: an Infino-backed index whose text field the table cannot analyze
 * as the mapping asks is refused there ({@link InfinoAnalyzers#validate}).
 * The tree for earlier releases holds this class without the check.
 */
abstract class InfinoPluginBase extends Plugin {
    @Override
    public Collection<IndexCreationValidator> getIndexCreationValidators() {
        return List.of(InfinoAnalyzers::validate);
    }
}
