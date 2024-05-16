<!--

NOTE: Please follow the instructions below when submitting a pull request
      For dbt Cloud Pull Request:
      * If you are trying to test your branch, submit a pull request and dbt Cloud will kick-off a run to validate.
      * If you are trying to **draft** a pull request, please select "Draft" so as to avoid dbt Cloud kicking-off and unnecessary job.
-->

# Description

Please include a summary of the change, including any relevant background, motivation, and context.

If relevant, include a description, screenshots, etc. of new behavior/model/update...

### Jira ticket(s): []()


# Testing story

- [ ] Does your change include appropriate tests on key columns?
      eg.
      - `not_null`
      - `unique`
      - `dbt_utils.unique_combination_of_columns: , ["value","value","value"...]

## **Note: when submitting a new model for review please make sure the following have been tested:**

- [ ] The model compiles 
  
      eg. `dbt build -m 'your_model'`
- [ ] The model runs 
      
      eg. (`dbt run -m 'your_model'`)

- [ ] The deployment run passed 
  
      see CI/CD job in dbt Cloud or Validation Chekcs (Below)

- [ ] The model produces accessible data in the DW 
   
  eg. `select 1 from 'your_model'`

## Privacy

- [ ] 1.	Does this change involve the collection, use, or sharing of new Personal Data?
  
- [ ] 2.    Do these data exist in the appropriate schema(s)? 

- [ ] 3.	Does this change involve a new or changed use or sharing of existing Personal Data?

- [ ] 4.    Will this data be visible on Tableau/ surfaced in a report/ exported from Trevor?

 
      If yes to any of the above, please list the models, columns, and justification below:

      i.
      ii.
      iii. 


## PR Checklist:
**Note: if these are not all checked, the PR will be sent back.**
- [ ] Pull Request is labeled appropriately (eg. `chore/`, `feature/`, `fix/`)

- [ ] Tests provide adequate coverage

- [ ] Privacy and Security impacts have been assessed

- [ ] Code adheres to 
  - [ ] [Our Style Guide](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects) 
  - [ ] **[DRY](https://docs.getdbt.com/terms/dry)**-ness
  
- [ ] New features are translatable or updates will not break up/downstream models

- [ ] Code is well-commented 
      
      Note: Please do not leave extraneous commentary in model code, if it is for the purpose of documentation, please relocate accordingly.

- [ ] Appropriate documentation has been provided 
  
      dbt_project.yml, _your_models.yml, etc.

- [ ] Relevant documentation has been updated 
  
      1. `dbt docs generate` has succeeded
      [Our docs site has been updated](https://code-dot-org.github.io/analytics/cdo_analytics)


- [ ] Follow-up work items (including potential tech debt) are tracked and linked (if applicable)

      i.
      ii.
      iii.

      
![Happy Coding!](https://www.codecademy.com/resources/blog/wp-content/uploads/2024/01/Frame-712.png)


<!--
changelog:
auth.      descr.      date
js         init        2024-01-22              
js         v1.2        2024-02-06
js         v1.3        2024-05-15

Misc. Resources:

https://www.codecademy.com/resources/blog/wp-content/uploads/2024/01/Frame-707.png 
The background color is `#ffffff` for light mode and `#000000` for dark mode.

-->
