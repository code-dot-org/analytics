NOTE: Please follow the instructions below when submitting a pull request
      For dbt Cloud Pull Request:
      * If you are trying to test your branch, submit a pull request and dbt Cloud will kick-off a run to validate.
      * If you are trying to **draft** a pull request, please select "Draft" so as to avoid dbt Cloud kicking-off and unnecessary job.
***
# Description

Please include a summary of the change, including any relevant background, motivation, and context.

If relevant, include a description, screenshots, etc. of new behavior/model/update/...

## Links

Jira ticket(s): []()

## Testing story

- [ ] Does your change include appropriate tests on key columns?
      eg.
      - `not_null`
      - `unique`
      - `dbt_utils.unique_combination_of_columns: , ["value","value","value"...]

**Note: when submitting a new model for review please make sure the following have been tested:**


1. The model compiles (`dbt build -m 'your_model'`)
         or: _has the dbt Cloud job succeeded?_
3. The model runs (`dbt run -m 'your_model'`)
4. The model produces accessible data in the DW (`select 1 from 'your_model'`)

## Privacy

- [ ] 1.	Does this change involve the collection, use, or sharing of new Personal Data?
- [ ] 2.    Do these data exist in the appropriate schema(s)? 
- [ ] 3.	Does this change involve a new or changed use or sharing of existing Personal Data?
- [ ] 4.    Consider: will this data be visible on Tableau? will this data be surfaced in a report exported from Trevor?
- [ ] 5.    If yes to any of the above, please list the models, columns, and justification below:
      i.
      ii.
      iii. 


## PR Checklist:
--> **Note: if these are not all checked, the PR will be sent back.**

- [ ] Tests provide adequate coverage
- [ ] Privacy and Security impacts have been assessed
- [ ] Code adheres to [style guide](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects)👀 and is **[DRY](https://docs.getdbt.com/terms/dry)**
- [ ] Code is well-commented (**please do not leave extraneous commentary in model code, if it is for the purpose of documentation, please relocate accordingly)
- [ ] Appropriate documentation has been provided (see `.yml.`, did `dbt docs generate` succeed?)
- [ ] New features are translatable or updates will not break up/downstream models
- [ ] Relevant documentation has been added or updated (i.e. `dbt docs` has been updated successfully on [Github Pages](code-dot-org.github.io/analytics/)
- [ ] Pull Request is labeled appropriately (eg. `chore/`, `feature/`, `fix/`)
- [ ] Follow-up work items (including potential tech debt) are tracked and linked (if applicable)



***
updated 2024-01-22 ls
changelog:
auth.      descr.      date
js         init        2024-01-22              
js         v1.2        2024-02-06
