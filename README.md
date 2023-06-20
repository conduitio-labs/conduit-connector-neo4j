# Conduit Connector Neo4j

## General

The [Neo4j](https://neo4j.com/) connector is one of Conduit plugins. It provides both, a source and a destination Neo4j connector.

### Prerequisites

- [Go](https://go.dev/) 1.18+
- [Neo4j](https://neo4j.com/) 5.3.0+
- (optional) [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/compose-file/) V2

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command will handle starting and stopping docker container for you.

## Source

The Neo4j Source Connector connects to a Neo4j with the provided `uri`, `entityType`, `entityLabels` and `database` and starts creating records for each insert detected in entity elements.

Upon starting, the Source takes a snapshot of given entity elements in the database, then switches into polling mode. In polling mode, the connector constantly polls for new items ordering them by the `orderingProperty` and limiting by the `batchSize`.

> **Note**
>
> The values of the `orderingProperty` field must be unique and sortable.

### Snapshot capture

When the connector first starts, snapshot mode is enabled. The connector reads all elements with `entityLabels` in batches using a cursor-based pagination, limiting the elements by `batchSize`. The connector stores the last processed element value of an `orderingProperty` in a position, so the snapshot process can be paused and resumed without losing data. Once all elements in that initial snapshot are read the connector switches into polling mode.

This behavior is enabled by default, but can be turned off by adding `"snapshot": false` to the Source configuration.

### Polling

The connector supports only insert operations by polling for new elements. The polling process is also resumable.

### Configuration

| name               | description                                                                                                                                                                                                                        | required |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `uri`              | The URI pointed to a Neo4j instance.                                                                                                                                                                                               | **true** |
| `entityType`       | Defines an entity type the connector should work with.<br/>The possible values are: `node` or `relationship`.                                                                                                                      | **true** |
| `entityLabels`     | Holds a list of labels belonging to an entity.<br/>- If the `entityType` is `node`, this field can accept multiple labels separated by a comma;<br/>- If the `entityType` is `relationship`, this field can accept only one label. | **true** |
| `orderingProperty` | The name of a property that is used for ordering nodes or relationships when capturing a snapshot.                                                                                                                                 | **true** |
| `database`         | The name of a database to work with.<br/>The default value is `neo4j`.                                                                                                                                                             | false    |
| `auth.username`    | The username to use when performing basic auth.                                                                                                                                                                                    | false    |
| `auth.password`    | The password to use when performing basic auth.                                                                                                                                                                                    | false    |
| `auth.realm`       | The realm to use when performing basic auth.                                                                                                                                                                                       | false    |
| `keyProperties`    | The list of property names that are used for constructing a record key.                                                                                                                                                            | false    |
| `batchSize`        | The size of an element batch.<br/>The min is `1`, and the max is `100000`. The default value is `1000`.                                                                                                                            | false    |
| `snapshot`         | Determines whether or not the connector will take a snapshot of all nodes or relationships before starting polling mode.<br/>The default value is `true`.                                                                          | false    |

### Key handling

The connector uses all fields from the `keyProperties` to construct a record key. If the field is empty the `orderingProperty` is used.

## Destination

The Neo4j Destination takes an `sdk.Record` and parses it into a valid Neo4j query.

### Configuration

| name            | description                                                                                                                                                                                                                        | required |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------- |
| `uri`           | The URI pointed to a Neo4j instance.                                                                                                                                                                                               | **true** |
| `entityType`    | Defines an entity type the connector should work with.<br/>The possible values are: `node` or `relationship`.                                                                                                                      | **true** |
| `entityLabels`  | Holds a list of labels belonging to an entity.<br/>- If the `entityType` is `node`, this field can accept multiple labels separated by a comma;<br/>- If the `entityType` is `relationship`, this field can accept only one label. | **true** |
| `database`      | The name of a database to work with.<br/>The default value is `neo4j`.                                                                                                                                                             | false    |
| `auth.username` | The username to use when performing basic auth.                                                                                                                                                                                    | false    |
| `auth.password` | The password to use when performing basic auth.                                                                                                                                                                                    | false    |
| `auth.realm`    | The realm to use when performing basic auth.                                                                                                                                                                                       | false    |

### Relationship creation handling

While the payload can contain any fields, two required fields, `sourceNode` and `targetNode`, must be present within a record payload for the destination to insert relationships correctly.

The `sourceNode` and `targetNode` are objects that have the following fields:

```js
{
  // the list of node labels
  "labels": ["Person"],
  // the map of key and values to match a node when creating a relationship
  "key": {
    "id": 1
  }
}
```

### Key handling

The connector supports composite keys and expects that the `record.Key` is structured when updating and deleting documents.
