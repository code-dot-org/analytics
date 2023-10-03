## Description

Please include a summary of the change, including any relevant background, motivation, and context.

If relevant, include a description, screenshots, etc. of new behavior/model/update/...


## Links

Jira ticket(s): []()

## Testing story

- [ ] Does your change include appropriate tests?
      eg. `not_null`, `unique`

**Note: when submitting a new model for review please make sure the following have been testing:
1. The model compiles (`dbt compile -m 'your_model'`)
2. The model runs (`dbt run -m 'your_model'`)
3. The model loads produces accessible data (`select 1 from 'your_model'`)

## Follow-up work

Any related, outstanding, blocking, or follow-up work:


## Privacy

- [ ] 1.	Does this change involve the collection, use, or sharing of new Personal Data?


- [ ] 2.	Does this change involve a new or changed use or sharing of existing Personal Data?


## PR Checklist:

- [ ] Tests provide adequate coverage
- [ ] Privacy and Security impacts have been assessed
- [ ] Code adheres to style-guide and is DRY
- [ ] Code is well-commented
- [ ] New features are translatable or updates will not break up/downstream models
- [ ] Relevant documentation has been added or updated
- [ ] Pull Request is labeled appropriately
- [ ] Follow-up work items (including potential tech debt) are tracked and linked
