# Conduit Connector Neo4j

## General

The [Neo4j](https://neo4j.com/) connector is one of Conduit plugins. It provides both, a source and a destination Neo4j connector.

### Prerequisites

- [Go](https://go.dev/) 1.18+
- [Neo4j](https://neo4j.com/) 5.3.0+
- [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/compose-file/) V2

### How to build it

Run `make build`.

### Testing

Run `make test` to run all the unit and integration tests, which require Docker to be installed and running. The command will handle starting and stopping docker container for you.

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
