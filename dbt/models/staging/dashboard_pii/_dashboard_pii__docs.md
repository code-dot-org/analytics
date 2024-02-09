version: 2

models: 
    - name: stg_dashboard_pii__users
        description: user fields which are pii

        columns:
        - name: races
            description: raw value for race 
            tags: 
            - contains_pii

        - name: race_group
            description: race categorization column
            tags: 
            - contains_pii
        
        - name: gender 
            description: raw value for gender
            tags: 
            - contains_pii
        
        - name: gender_group
            description: gender categorization column
            tags: 
            - contains_pii