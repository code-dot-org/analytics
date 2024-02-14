{#
Experimental model

Based on feedback / insight from Sean who discovered that a bunch of teachers we had flagged as churned for CSA
claimed to be activily teaching CSA.  A look into those 6 teachers and associated student activity showed:

The reason we had them churned is that they all had a section with < 5 students who had csa activity
HOWEVER, 
1. they did ALL have a section (with a name suggesting CSA)
2. they HAD STUDENTS with csa activity, albeit less than 5
3. the had all ASSIGNED a CSA script to the section
4. in most cases the activity in the section was exclusively CSA activity

This makes me want to experiment with teacher/student activity model that:

1. ignores the 5+ students in a section rule
2. derives course taught from the script/course assigned (rather than from student course activity - though maybe we could use each to validate the other)
3. Uses student enrollment in the section per school year and general level activity for metrics of being active

This model would suggest the CORE ACTION of a teacher (teacher engagment/acquisition) is creating a section, assigning a script/course, and enrolling students.

It would also mean we'd REALLY have to instrument a section-script-changelog and invent a way to make judgements about which course was the "real" course 
in the event that a teacher changes it.  For example, I'd like to know how many days a script/course is left as the assigned course for the section.

#}

with teachers as (
    select *
    from {{ref('dim_teachers')}}
)