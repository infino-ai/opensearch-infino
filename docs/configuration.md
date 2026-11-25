# Configuration

**None of this has to change for a default run.** Every setting below has a default that works as it is. The [quick start](../README.md#quick-start) and the [getting-started guide](getting-started.md) don't ask you to set any of them: every index the cluster creates is Infino-backed, with its table compacted every 60 seconds, and the guide's account step fills in the account settings for you. Where the tables live is the one choice a node makes, and `INFINO_STORAGE` in Docker Compose (or `run.sh`) makes it (see [where the tables live](../README.md#install-on-a-cluster)).

Come here when you want to:

- change how a single index is served (proxy mode, a named table, a graph of edges);
- read a table loaded outside OpenSearch, without reindexing it ([Attaching to an existing table](#attaching-to-an-existing-table));
- refuse searches that would read a very large table whole ([Very large tables](#very-large-tables));
- tune the node: its hosted account, its cache, its write batches.

**Contents:** [Index settings](#index-settings) · [Node settings](#node-settings-opensearchyml) · [JVM properties](#jvm-properties-configjvmoptions-or-opensearch_java_opts) · [Attaching to an existing table](#attaching-to-an-existing-table) · [Very large tables](#very-large-tables)

## Index settings

| Setting | Default | |
|---|---|---|
| `index.infino.enabled` | on (`infino.index.default_enabled`) | Whether the index is Infino-backed. |
| `index.infino.mode` | `default` | `default` or `proxy`; fixed at creation. See [How it works](../README.md#how-it-works). |
| `index.infino.table` | derived from the index's name | The table behind the index, named explicitly, to reattach a recreated index to its rows. A named table is kept when its index is deleted. |
| `index.infino.attached` | `false` | The table named in `index.infino.table` was made outside the plugin: it is read as it is and never changed. Fixed at creation. See [Attaching to an existing table](#attaching-to-an-existing-table). |
| `index.infino.search.window.field` | none | The date field a search's span of time is read from, for `index.infino.search.window.max`. Dynamic. |
| `index.infino.search.window.max` | none | The widest span of that field one search may list documents or break them into groups over, such as `1d`. A wider search, or one that names no range with both ends, is refused at once with a 400, and the table is not sent it. A count, and a sum, average, minimum, maximum or value count with no hits beside them, are answered over any span. The span is measured as the statement reads it: each end in the range's format or the field's, an inclusive upper end to the last instant of what it names (`now/y` to `now/y` is a year). `_delete_by_query`, `_update_by_query` and `_reindex` search first, so on such an index they need the same range. The SQL routes are not held to it. For a table too large to read whole on a visitor's request, where such a search would run for minutes and hold a search thread of the node until it ends. Dynamic. |
| `index.infino.search.phrases` | `true` | Whether a search may ask for a phrase on a text field (`match_phrase`, a `multi_match` of type `phrase`, a quoted run of a query string), in its query, its post filter, a filter aggregation or a rescore. Words reach the table as a filter beside the search's other clauses; a phrase reaches it as a lookup of its own over every row, which no range narrows, and on a very large table that lookup does not finish. Set to `false`, such a search is refused at once with a 400, a count included. A phrase query on a keyword, number or date is a comparison and is always served. Independent of the window settings. Dynamic. |
| `index.infino.optimize_interval` | `60s` | How often the table is compacted (5 seconds at least; off for a hosted account, which compacts its own tables). |
| `index.infino.edges` | `false` | The index holds a graph's edges, for `graph_walk` and `graph_rank` in [SQL](../README.md#sql). Fixed at creation. |

Two more index settings belong to the plugin, and no request sets them: `index.infino.rebuilt` marks an index a node recreated from the registry over a table that already held rows (see [Stateless nodes](../README.md#hosted-account-and-stateless-nodes)), and the private `index.infino.table_claimed` records whether the index was given its table when it was created (see the next paragraph).

A table backs one live index. If two indices name the same table (explicitly, or by names that map to one table, such as `logs.app` and `logs_app`), the older one is served from it and the other is refused, even after the older one is deleted. With the security plugin, setting `index.infino.table` in a create, resize, rollover or template request needs the right to manage the cluster.

## Node settings (`opensearch.yml`)

| Setting | Default | |
|---|---|---|
| `infino.index.default_enabled` | `true` | Whether new indices are Infino-backed unless their settings say otherwise. |
| `infino.default_mode.enabled` | `true` (`false` when neural search is installed) | Whether this node serves default-mode indices. Off: its indices are created in proxy mode. |
| `infino.platform.url` | none | The hosted service the tables live on. |
| `infino.platform.database` | the cluster's name | The account's database. |
| `infino.platform.api_key` | none | The account's key, in the keystore. `INFINO_API_KEY` in the environment is read when it is unset. |
| `infino.platform.default_url` | `https://api.platform.infino.ws` | The service a trial or a `PUT` of the account is sent to when the request names none. |
| `infino.platform.read_window` | `1s` | How long a read on the hosted service may be answered from a snapshot taken before a write. A refresh waits this long after the last write it covers, so the search after it sees the writes. `0` waits for nothing. Not used with the node's own storage. |
| `infino.cache.dir` | `infino-cache` under the node's first data path | Where the engine keeps a local copy of what it reads from the node's storage. Not used with a hosted account. |
| `infino.sql.max_result_size` | `http.max_content_length` | The largest SQL result, as JSON; a statement whose result is larger is refused with a 400. |
| `infino.skeleton_rebuild.paused` | `false` | Dynamic (`PUT _cluster/settings`). Holds the skeleton rebuild of an index recreated over a table that has rows, until it is cleared. Meanwhile the index's searches miss those rows, and writes look ids up in the table. |

## JVM properties (`config/jvm.options` or `OPENSEARCH_JAVA_OPTS`)

| Property | Default | |
|---|---|---|
| `infino.lib` | `libinfino_capi.so` on the library path | The native bridge. On macOS, name the `.dylib`. |
| `infino.uri` | `memory://` | Where the tables live without an account: `file://`, `s3://` or `gs://`. |
| `infino.engine.batch.docs` | `50000` | Default mode: the most documents a shard's batch holds before it commits on its own, between refreshes. |
| `infino.engine.batch.bytes` | 64 MiB | Default mode: the most source bytes a shard's batch holds before it commits on its own. |
| `infino.engine.copies.wait.ms` | `60000` | Default mode: how long a refresh, flush or hand-off waits for the shard's copies to hold the operations it covers. |
| `infino.batch.docs` | `50000` | Proxy mode: the most documents one write batch holds. |
| `infino.batch.bytes` | 64 MiB | Proxy mode: the most bytes one write batch holds. |
| `infino.batch.concurrency` | half the `write` pool | Proxy mode: the most appends to one table that run at once. Appends that name the same document id never run at once. |
| `infino.nested.candidates.max` | `10000` | The most documents a `nested` query may leave to check entry by entry; past it the query is refused. |

## Attaching to an existing table

An index can read a table that was made and filled outside the plugin, such as one loaded through the engine or the hosted service:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -X PUT https://localhost:9200/pageviews -H 'Content-Type: application/json' -d '{
  "settings": { "index.infino.table": "pageviews", "index.infino.attached": true },
  "mappings": { "properties": {
    "time":  { "type": "date" },
    "title": { "type": "text" },
    "hits":  { "type": "long" }
  } }
}'
```

The mapping names the table's columns and the field type each is read as. It is checked against the table when the index is created: a table that does not exist, a field the table has no column for, or a column of another type, is refused with a 400 and no index is created. The settings and mappings of the templates the index's name matches are judged with the request's, as the cluster merges them. A table that cannot be read at that moment is not judged: the index is created, and answers each request with the reason once its shard has read the table.

- **The table is never changed.** Nothing is added to it when the index is created, it is not compacted, and it stays when the index is deleted.
- **Every write is refused** with a 403: index, update, delete, a write by query, and a document an ingest pipeline sends to the index from another one. A bulk request with an item for an attached index is refused as a whole, its items for other indices included; send those in a bulk of their own.
- **A template may set `index.infino.attached` and leave the table to the request** that creates the index. The two are checked together when the index is created.
- **The mapping is fixed when the index is created.** A mapping update is refused with a 400. For a different mapping, delete the index and create it again: it holds no data of its own.
- **A document is a row.** Its `_id` is the engine's row id, and its `_source` is the row's columns; a null column is left out, and so is a vector or list column, which is searched but not returned. A date column is read as the instant its day starts at.
- **The index is served in proxy mode.** A request or a template that asks for default mode is refused. One table backs the whole index whatever its number of shards, so give it one shard.
- **A `text` field needs a full-text index on its column.** One without positions matches and ranks terms; a phrase query needs positions. A column with a full-text index is searched as text only when its field is mapped as `text`: mapped as `keyword`, or left out of the mapping, it is compared whole.
- **A table with a column named `score`, `osid`, `source` or `osseq` is not attached to.** A search gives each row's score under the first, and the plugin reads a document through the others.
- **A column the table's owner removes later** is noticed by a statement that fails on it, which then runs again over the columns the table holds now. A ranked search (`match`, k-NN, hybrid) does not: create the index again after such a change.
- A table the plugin wrote is not attached to. Name it in `index.infino.table` without this setting.

## Very large tables

On a table of billions of rows, some searches read every row: listing hits over all time, grouping by terms over all time, or a phrase, which the table looks up over every row whatever range the search names. Such a search can run for minutes and hold one of the node's search threads until it ends. Three dynamic index settings refuse these at once with a 400, before the table is sent anything:

```bash
curl -sk -u "admin:$OPENSEARCH_ADMIN_PASSWORD" -X PUT https://localhost:9200/pageviews/_settings -H 'Content-Type: application/json' -d '{
  "index.infino.search.window.field": "time",
  "index.infino.search.window.max": "1d",
  "index.infino.search.phrases": false
}'
```

- **`index.infino.search.window.max`** with **`index.infino.search.window.field`**: a search that lists hits or breaks them into groups must name a range on that field, with both ends, no wider than the maximum. The refusal says the span that is allowed. A count, and a sum, average, minimum, maximum or value count with no hits beside them, are answered over any span.
- **`index.infino.search.phrases: false`**: a phrase query on a text field is refused, a count included. Words are still served.

The [index settings](#index-settings) table gives each in full. They suit an [attached](#attaching-to-an-existing-table) table that a visitor searches through the node.
