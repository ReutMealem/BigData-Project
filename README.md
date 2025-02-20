# Big Data Final Project

This repository contains the codebase for the Big Data Final Project, focusing on processing and analyzing OpenAlex academic data enriched with war-related information.

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
## Usage
To run the main data processing script:
```bash
python src/process_war_data.py <task_id>
```

## Contact
For questions or contributions, contact [your_email@example.com].
