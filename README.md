# code-dot-org/analytics

Hello friend,

Welcome to the cdo-analytics dbt codebase. 

The following models and configurations are used to transform, load, and model data from our studio application (amongst other third party sources) into our data warehouse. 

Our anticipated release schedule is as follows:

## Release Name (Release Date)
0. [Pre-Release] **Hydrone** v1.0.2023   2023-12-15
1. [Release 1] **Hydrone** v1.1.2024     2024-02-05


### Using dbt
For starters, you should set-up a local dbt-core install or dbt Cloud environment.

After you set up dbt, try running the following commands:

* `dbt clean && dbt deps`
* `dbt debug`
* `dbt run --select /dbt/models/marts/*`

#### Resources:
Our [style guide](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects) (for the most part...)
* For best practices in our design and implementation.
* For our **sql** style-guide, we follow [Gitlab's standards](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/sql-style-guide/)
* If you really want to [nerd out...](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/)

* Join the chat on [Slack](getdbt.slack.com) for live discussions and support
* Check out the [blog](https://docs.getdbt.com/blog) for the latest news on dbt's development and best practices
