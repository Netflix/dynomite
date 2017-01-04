## Configuring Dynomite
Please refer to README.md for more details.

## Token Management
Dynomite is based on the Dynamo protocol where nodes serve keys that belong inside a token range. Differently from Cassandra, each Rack contains the full ring. Hence in RF=3 configuration the full ring is deployed in 3 Racks, whereas in RF=2 the full ring is deployed in 2 racks.

To create tokens you can use this script to generate [YAMLS](https://github.com/Netflix/dynomite/blob/dev/scripts/dynomite/generate_yamls.py)

Exemplar Configuration YAML files can be found in the [conf directory](https://github.com/Netflix/dynomite/tree/dev/conf)