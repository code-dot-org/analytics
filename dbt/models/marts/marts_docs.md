{% docs dim_school_status_status %} 
### Active status of a school in a given school year. Can be one of the following: 
- Market- The school is not currently active and has never been active.
- Inactive Churn- The school is not currently active, was not active in the previous year, but has been active in the past.
- Inactive This Year- The school is not currently active but was active in the previous year.
- Active New- The school is currently active, was not active in the previous year, and has never been active before.
- Active Reacquired- The school is currently active, was not active in the previous year, but has been active in some previous years.
- Active Retained- The school is currently active and has been active in the previous year.
{% enddocs %}


{% docs fct_monthly_accounts_created %}
### Accounts created by month
- Scope: user account was created in the given month (and year)
- Segmented by student/
- Excludes "dummy accounts" (student accounts created but don't have a sign_in attempt)
- Use Case: teacher creates a section of picture accounts many of which don't get used.
- NOTE: right now delted accounts are filtered out at the base_users table.  Waiting on a change
to that in order to make these counts accurate.
{% enddocs %}

