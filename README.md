# :soccer: Palpiteiro Data
Data models for the fantasy soccer app.

![dbt](https://img.shields.io/badge/dbt-FF694B?&logo=dbt&logoColor=white)
![gcp](https://img.shields.io/badge/Google_Cloud-4285F4?&logo=google-cloud&logoColor=white)

[![build](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/build.yml/badge.svg)](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/build.yml)
[![freshness-main](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/sources.yml/badge.svg)](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/sources.yml)
[![Materialize dim_player_last](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/dim_player_last.yml/badge.svg)](https://github.com/matheusccouto/palpiteiro-data/actions/workflows/dim_player_last.yml)






### Data Warehouse
The data warehouse is Google Cloud Big Query and it is built with DBT. [Read the docs.](https://matheusccouto.github.io/palpiteiro-data)

![dbt-dag](img/dbt-dag.png)

The model **dim_player_last** is the main one. It has all features (player, club and points estimatives) for the players available for the next round. It has a remote function (which is a Google Cloud Function) that receives features from the model **fct_player** and uses **LightGBM** to predict how many points each player will score in the next round.

![dbt-dag-exposures](img/dbt-dag-exposures.png)

You can interact with this lineage graph in the [DBT Docs](https://matheusccouto.github.io/palpiteiro-data).
