# code-dot-org/analytics

Hello friend,

Welcome to the code.org analytics codebase! 

The following models and configurations are used to transform, load, and model data from our studio application (and other third party sources) into our data warehouse. 

Our anticipated release schedule is as follows:

### Release Log 🛳️                              Shipped:
0. [Pre-Release]  **Hydrone** v1.0.2023     [2023-12-15]
1. [Release 1]    **Hydrone** v1.1.2024     [2024-02-02]


#### Using dbt
For starters, you should set-up a local dbt-core install or dbt Cloud environment.

After you set up dbt, try running the following commands:

* `dbt clean && dbt deps`
* `dbt debug`
* `dbt run && dbt docs generate`

#### Resources:
* Please refer to our `sql`/`dbt` [style guide](https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models) for building models
* For a more detailed guide on buildilng our `dbt-project` see [here](https://handbook.gitlab.com/handbook/business-technology/data-team/platform/dbt-guide/)
* Join the chat on [Slack](getdbt.slack.com) for live discussions and support
* Check out the [blog](https://docs.getdbt.com/blog) for the latest news on dbt's development and best practices
