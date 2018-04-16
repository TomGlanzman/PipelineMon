# PipelineMon
Example for querying the SLAC Pipeline database and summarizing in a text format suitable for web display.  For the web display, this code depends on the publicly available 'tabulate' package.

This example queries the Pipeline database for the 12 workflow tasks that constitute the DESC DC2 Run 1.2p project.  Each workflow corresponds to either the wide-field deep (WFD) or the ultra deep-drilling field (uDDF), then separate workflow for the six optical filters {u,g,r,i,z,y}.  The output of the example is stored in my SLAC public html directory and can be viewed here: https://www.slac.stanford.edu/~dragon/DESC/DC2mon.html

Note that the database query is confined to the 'query.sql' file, which was created by Brain Van Klaveren.  The selection of data queried was motivated by the need to monitor DC2 phoSim workflows; other data should be available with modifications in the query.  In addition, Brian advised that user-defined "pipeline variables", "stream variables" and "job instance variables" could also be available, but this example does not provide a template recovering those data types.
