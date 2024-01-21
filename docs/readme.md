## Readme: How to Deploy dbt docs
## Author(s): js, ag
## Last Update: 2024-01-21

  In DBT cloud, click into the latest successful deploy run
Click artifacts
Download all (7) of the .json files and the index.html file
Sources.json
Semantic_manifest.json
Run_results.json
Manifest.json
Index.html
Graph_summary.json
Catalog.json
I think this is all the files to download, but not 100% positive. I believe that all the .sql files in the compiled/ directory will be generated for you when you run “dbt docs serve”
In the local repo, make a folder called target (~/dbt/target/)/
Copy all of the downloaded files into that folder
Run dbt docs serve
Verify that the local docs server is working as expected
Rename replace contents of docs/ folder with the contents of the target/ folder
Commit and push changes
Once new docs/ folder is merged into main, github will automatically update the docs site
