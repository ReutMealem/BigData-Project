# Big Data Final Project

This repository contains the codebase for the Big Data Final Project, focusing on processing and analyzing OpenAlex academic data enriched with war-related information.

## Abstract
Armed conflicts have profound effects on academic mobility, disrupting research environments and compelling scholars to migrate. This study examines how wars influence research output at both the country and institutional levels, utilizing large-scale datasets from OpenAlex and Wikipedia. Through statistical analysis, data visualization, and network modeling, findings reveal that while country-level research output remains largely stable during conflicts, institutional-level trends exhibit more nuanced disruptions. Certain institutions experience increased research activity due to shifts in research priorities or funding, while others face declines, reflecting disruptions in civilian research. Network analysis using NetworkX and Gephi highlights changes in institutional collaborations, with wartime periods leading to more centralized research structures. These insights emphasize the need for policies that support displaced academics and ensure research continuity in conflict-affected regions.


Findings show that while country-level analysis did not reveal significant disruptions in overall research output during war periods, institutional-level trends showed more nuanced patterns. Certain institutions experienced fluctuations in authorship counts, with some seeing increased activity during wartime, likely due to shifts in research priorities or funding, while others exhibited declines, suggesting disruptions in civilian or non-military research efforts.

## Visualization

### Institutional Research Network Comparison
Below is a visual comparison of institutional research network structures in 1993 (no war) and 2003 (war period) using modularity analysis.

![Institutional Research Network Comparison](/image/inst_graph.png)

**Figure:** Comparison of institutional research network structures in 1993 (no war) and 2003 (war period) using modularity analysis. 
- **Top row:** Network visualizations show institutional collaborations, where node colors represent modularity classes, node sizes reflect delta-degree values (difference between in-degree and out-degree), and the black square represents the Physico-Technical Institute node.
- **Bottom row:** Modularity class distributions for each year. In 1993, the network exhibits multiple distinct modularity classes, indicating a decentralized collaboration structure. In 2003, the network becomes more centralized, with the Physico-Technical Institute absorbing a significant portion of institutions, reducing the number of prominent modularity classes. 

This visualization demonstrates how armed conflict affects institutional collaboration structures, leading to more centralized networks during wartime.


## Project Structure
```
code/
│
├── .git/                        # Git repository metadata
├── analyzed_data/               # Analysis notebooks and frames
│   ├── analyzed_data_by_country.ipynb
│   ├── analyzed_data_by_institution.ipynb
│   ├── frames/                  # Generated frames
│   └── additional_analysis/     # Additional analysis scripts
├── data mining/                 # Data processing scripts
│   ├── create_df_from_sql.py
│   ├── merge_all_dfs.py
│   ├── openAlex_data_api_sql.ipynb
│   └── wiki_war_data.ipynb
├── preprocess_data/             # Data preprocessing scripts
│   ├── preprocess_data.ipynb    # Preprocessing notebook
│   └── additional_preprocessing/ # Additional preprocessing scripts
├── sbatch_files/                # Batch processing scripts
├── params.py                    # Parameter configuration
├── const.py                     # Const configuration
└── README.md                    # Documentation
```

## Main Scripts
- `process_war_data.py`: Adds war-related metadata to academic publication data.
- `create_df_from_sql.py`: Extracts and processes data from SQL databases.
- `merge_all_dfs.py`: Merges multiple dataframes for analysis.
- `preprocess_data.ipynb`: Preprocesses OpenAlex data before analysis.
- `openAlex_merged_data_chunks.py`: Processes OpenAlex data in chunks.

## Notebooks
- `wiki_war_data.ipynb`: Analyzes Wikipedia war data.
- `openAlex_data_api_sql.ipynb`: Fetches and processes OpenAlex data via API and SQL.
- `analyzed_data_by_country.ipynb`: Analyzes OpenAlex data by country.
- `analyzed_data_by_institution.ipynb`: Analyzes OpenAlex data by institution.
