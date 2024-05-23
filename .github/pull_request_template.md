<!--

NOTE: Please follow the instructions below when submitting a pull request
      For dbt Cloud Pull Request:
      * If you are trying to test your branch, submit a pull request and dbt Cloud will kick-off a run to validate.
      * If you are trying to **draft** a pull request, please select "Draft" so as to avoid dbt Cloud kicking-off and unnecessary job.
-->

# Description

Please include a summary of the change, including any relevant background, motivation, and context.

<<<<<<< HEAD
If relevant, include a description, screenshots, etc. of new behavior/model/update...

### Jira ticket(s): []()


# Testing story
=======
If relevant, include a description, screenshots, etc. of new behavior/model/update/...

## Links

Jira ticket(s): []()

## Testing story
>>>>>>> d74a01820b0075e0deb853d376da694b228f42a0

- [ ] Does your change include appropriate tests on key columns?
      eg.
      - `not_null`
      - `unique`
      - `dbt_utils.unique_combination_of_columns: , ["value","value","value"...]

<<<<<<< HEAD
## **Note: when submitting a new model for review please make sure the following have been tested:**

- [ ] The model compiles 
  
      eg. `dbt build -m 'your_model'`
- [ ] The model runs 
      
      eg. (`dbt run -m 'your_model'`)

- [ ] The deployment run passed 
  
      see CI/CD job in dbt Cloud or Validation Checks (Below)

- [ ] The model produces accessible data in the DW 
   
  eg. `select 1 from 'your_model'`
=======
**Note: when submitting a new model for review please make sure the following have been tested:**

1. The model compiles (`dbt build -m 'your_model'`)
         or: _has the dbt Cloud job succeeded?_
3. The model runs (`dbt run -m 'your_model'`)
4. The model produces accessible data in the DW (`select 1 from 'your_model'`)
>>>>>>> d74a01820b0075e0deb853d376da694b228f42a0

## Privacy

- [ ] 1.	Does this change involve the collection, use, or sharing of new Personal Data?
<<<<<<< HEAD
  
- [ ] 2.    Do these data exist in the appropriate schema(s)? 

- [ ] 3.	Does this change involve a new or changed use or sharing of existing Personal Data?

- [ ] 4.    Will this data be visible on Tableau/ surfaced in a report/ exported from Trevor?

 
      If yes to any of the above, please list the models, columns, and justification below:

=======
- [ ] 2.    Do these data exist in the appropriate schema(s)? 
- [ ] 3.	Does this change involve a new or changed use or sharing of existing Personal Data?
- [ ] 4.    Consider: will this data be visible on Tableau? will this data be surfaced in a report exported from Trevor?
- [ ] 5.    If yes to any of the above, please list the models, columns, and justification below:
>>>>>>> d74a01820b0075e0deb853d376da694b228f42a0
      i.
      ii.
      iii. 


## PR Checklist:
**Note: if these are not all checked, the PR will be sent back.**
<<<<<<< HEAD
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
=======

- [ ] Tests provide adequate coverage
- [ ] Privacy and Security impacts have been assessed
- [ ] Code adheres to [style guide](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects)👀 and is **[DRY](https://docs.getdbt.com/terms/dry)**
- [ ] Code is well-commented (**please do not leave extraneous commentary in model code, if it is for the purpose of documentation, please relocate accordingly)
- [ ] Appropriate documentation has been provided (see `.yml.`, did `dbt docs generate` succeed?)
- [ ] New features are translatable or updates will not break up/downstream models
- [ ] Relevant documentation has been added or updated (i.e. `dbt docs` has been updated successfully on [Github Pages](code-dot-org.github.io/analytics/)
- [ ] Pull Request is labeled appropriately (eg. `chore/`, `feature/`, `fix/`)
- [ ] Follow-up work items (including potential tech debt) are tracked and linked (if applicable)

>>>>>>> d74a01820b0075e0deb853d376da694b228f42a0


<!--
changelog:
auth.      descr.      date
js         init        2024-01-22              
js         v1.2        2024-02-06
<<<<<<< HEAD
js         v1.3        2024-05-15

Misc. Resources:

https://www.codecademy.com/resources/blog/wp-content/uploads/2024/01/Frame-707.png 
The background color is `#ffffff` for light mode and `#000000` for dark mode.

=======
>>>>>>> d74a01820b0075e0deb853d376da694b228f42a0
-->
