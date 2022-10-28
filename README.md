# Mini-Challenge 1 - High Performance Computing (hpc) HS22


This repository contains the code for the first mini-challenge of the 4 ECTS module `hpc` from the bachelor degree course in [Data Science at FHNW](https://www.fhnw.ch/en/degree-programmes/engineering/bsc-data-science).

## Task description:
The task description for this  mini-challenge is in the file: **Task_description.md**.

## Documentation
The documentation of the work steps that have been carried out can be found in the notebook: **Report.ipynb**

Startup:
1. Download the [Toys and Games 5-core](https://nijianmo.github.io/amazon/index.html) Amazon reviews dataset from no jianmo accessible on GitHub and put it into a src/json folder in the source directory.
2. Run the docker compose file to setup the docker container structure. This can be done with the command: *docker-compose up*. (For removal at the end use *docker-compose down*)
3. In the Jupyter1 container, you can start the producers by running the following notebooks: **Producer_1.ipynb** and **Producer_2.ipynb** The consumers can be started by running the notebooks: **Consumer_1_and_Datasink.ipynb** and **Consumer_2_and_Datasink.ipynb**
