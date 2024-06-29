{#

model: fct_rosetta_run_log
auth:  js
date: 2024-06-28

design:
    This model was developed to not only execute roseta along with Hydrone, but to also build a location where to see rosetta logs also.
    This model will be built incrementally (eventually) and utilizes a macro
    Hopefully, we can just rely on Hydrone going forward for KTLO work such as this
#}

CALL analysis.run_rosetta();

