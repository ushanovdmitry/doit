## DAG:
Creates and manages tasks, builds dependency graph, runs tasks in a particular order.

## Task:
Connects Action with Artifacts, manages _local_ dependencies.
Stores dependency data into backend: with which input arguments action was executed.
Checks if action should be executed.

## Action:
Provides _explicit_ dependencies. Can be executed.

## Artifact:
Label for a real artefact stored somewhere.
Provides functionality to check if artefact has changed: fingerprints.

## Backend:
Key-value storage.

