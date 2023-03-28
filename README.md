# mcdumpload

to build run `make`
to run: `./mcdumpload --help`

- Uses new `lru_crawler mgdump` command.
- Streams both key list and data from source host to destination host
- Leverages metaget command flags to efficiently grab full key data from source host (client flags, TTL remaining, avoids updating LRU, etc)
- Avoids having to track state by getting key back from metaget
- Avoids tracking final state by sending MN commands to source and destination when streaming data is complete.
- Minimizes processing within mcdumpload: avoids memory copying of value data. Edits metaget responses into metasets and forwards buffer to destination host.

## Usage

For including or excluding keys, use `--keyinclude` and `--keyexclude`
filters.

- The first filter specified sets whether or not all keys must be "included"
  or "excluded"
- Multiple filters may be specified: `--keyinclude="/foo" --keyinclude="bar"`
- Can exlcude a subset of keys: `--keyinclude="/foo/" --keyexclude="/foo/bar/"`
