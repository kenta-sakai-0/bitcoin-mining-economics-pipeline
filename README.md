<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](https://lookerstudio.google.com/s/kXABFCADhUY)

This project was created to track, analyze and compare unit economics for the following bitcoin miners: IREN, CLSK, BITF, CORZ, CIFR, MARA, BTDR, BTBT, RIOT, WULF, and HUT. The dashboard is hosted [here](https://lookerstudio.google.com/s/kXABFCADhUY).


**Data sources**:
* [Bitcoin 1-minute historical prices](https://www.kaggle.com/datasets/mczielinski/bitcoin-historical-data)
* [Hashrate](https://mempool.space/docs/api/rest#get-hashrate)
* [Income Statement](https://site.financialmodelingprep.com/developer/docs/financial-statement-free-api)
* [Balance Sheet](https://site.financialmodelingprep.com/developer/docs/balance-sheet-statements-financial-statements)
* [Cashflow](https://site.financialmodelingprep.com/developer/docs/cashflow-statements-financial-statements)
* [FX-rates](https://financialmodelingprep.com/stable/historical-price-eod)
<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With
* ![Python-shield]
* [![Dagster-shield]](https://github.com/dagster-io/dagster)
* [![dbt-shield]](https://github.com/dbt-labs/dbt-core)
* [![Docker-shield]](https://www.docker.com/)
<p align="right">(<a href="#readme-top">back to top</a>)</p>

### About the pipeline
![dagster-screenshot]
* The pipeline is orchestrated with Dagster.
* Data is ingested then pushed to BigQuery as source tables.
* Further transformations are performed with dbt
* Naming conventions loosely follow dbt best practices and are labeled `src`, `stg` and `int`. 



<!-- GETTING STARTED -->
## Getting Started
### Prerequesites
* [Install Docker Desktop](https://docs.docker.com/desktop/)
* Create GCP project
* Get a [free FMP API key](https://site.financialmodelingprep.com/register)

### Pipeline configuration
1. Create a `.env` file with the following variables and place it in the project root:
   - `GCP_PROJECT_ID`
   - `FMP_API_KEY`
   - `DBT_DATASET_NAME`

2. GCP Authentication:
   - Create a GCP Service Account and assign the following roles:
     - `BigQuery Data Editor`
     - `BigQuery Job User`
     - `Storage Object User`
   - Generate the Service Account JSON.
   - Update the file paths in `docker-compose.yaml` and `dagster.yaml` to point to the JSON file.
3. To start container:
  ```docker compose up --build ```  

<p align="right">(<a href="#readme-top">back to top</a>)</p>





<!-- ROADMAP -->
## Roadmap

- [x] Create FX rates dataset
- [ ] Use real-time block metrics to trigger event based refreshes
- [ ] Add historical insider ownership
- [ ] Add historical short interest

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[product-screenshot]: images/looker_dashboard.png
[dagster-screenshot]: images/dagster_sources.png
[Python-shield]: https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=fff
[Dagster-shield]: https://img.shields.io/badge/Dagster-Orchestration-blue?logo=dagster
[dbt-shield]: https://img.shields.io/badge/DBT-Analytics%20Engineering-ff694f?logo=dbt
[Docker-shield]: https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=fff
