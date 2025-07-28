Seth Lupo
seth.lupo@tufts.edu
0-Epsilon Query Estimation with LLMs
Tufts Security And Privacy Lab
July 28th, 2025

FIX MISPELLIGN AND PHRASE BETTER FOR SOMEHTING INLINE WITH PROFESSIONAL PRESENTATION.
LET ME KNOW ANY IMAGES YOU NEED IN COMMENTS AND ALSO IMAGE SUMMARY COMMENT AT END OF PRESENTATION (put images in images sbdirectory that i made)

SECTION IT INTO PROPER STRUCTURE FIT FOR RESEARCH PRESENTATION. THINK ABOUT IT AHEAD OF TIME

This slideshow should outlined what I have done in this experiment. And discuss ot a little.

Motivation: In order to optimize queries, databases track statistics about their tables. This is for join orders .. [come up with other examples]
this is done in pg_statistics (not human readable) and is visible via pg_stats. However, this leaks information about the underlying data that a honest but curious adversary coudld end up using.

My goal is to estimate these statistics without leaking information from the underlying tables. 

How to do this, well [[[explain the schneider ai method over many slides, the pipeline which is currently in the code]]]

[[Explain the test bench that i made, i will supply you with necessary screenshots]]

EXPLAIN THAT I USE against baseline methods like 

[[I ran the experiment and it resulting in all of the data currently in the SQL lite database. PLEASE QUERY this data for your analysis]]
[two important things are the entire trials of all the experiments, and also the first trial of each experiment (because all buffer and caches should be clean)].

Use chart-scripts to use a venv to make create necessarey graphes to put in images subdirectory.

Do some analysis on the data.

Conclude and add some conclusion slide

THIS SHOULD BE A FULL AND WELL STRUCTURED RESEARCH PRESENTATION
