/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.infino;

import org.opensearch.plugins.Plugin;

/**
 * The plugin's base before OpenSearch 3.7, which gives a plugin no check of
 * an index's mapping as it is created. A mapping update is still checked
 * ({@link InfinoAttachedMappingActionFilter}); the tree for later releases
 * holds this class with the check at creation.
 */
abstract class InfinoPluginBase extends Plugin {}
