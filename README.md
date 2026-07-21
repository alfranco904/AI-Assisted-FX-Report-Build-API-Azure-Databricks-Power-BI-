# powerbi-ai-assisted-dashboard
Creating a Power BI Dashboard, utilizing AI to assist in building, populating and laying it out 

Utilized AI to generate organization, product, account, GL Account and customer dimension files with a GL Fact.  Note that the data was generated 
for a fictional bank with fictional clients and branches.
I instructed the model to help construct a Power BI dashboard that would be useful for quarterly PNL and Balance Sheet review.

The model generated a set of steps and some preliminary files to use as the basis of the star schema, along with: 

1) Instructions on how to best perform the actiivty on my laptop
2) M to load the tables
3) Instructions for the relational model
4) Instructions for setting up DAX
5) Instructions for laying out the pages of the Dashboard (4)

I was able to apply my banking experience as well as my background in data modelling, ETL and profitability reporting to build the dashboard and obtain "unit test" level results.

There were a few challenges:
1) The data generated looked good on the surface, but there were some data quality challenges.
   
  for example:
  
    - The model genereated and spread the $ so the visualizatoins would work, but, in some cases, did it in a way that did not make business sense.
    - Some of the amounts were spread by the model in a way that clumped up the $$ in one product type.
    - I had to ask the model to take multiple passes at the data generation, but never felt that it got to a point where I had to completely start over.
    - I also had to manually cleanse and adjust some data. 
  
2) Some of the relationships, field names and their data population needed to be more consistent.
   
        -It still took some of data relationship refinement and data cleansing to obtain a reasonably useable dashboard.
3) I also had to tweak the instructions and code provided in order to achieve the desired result. Nothing "just ran." 

All the above said, the exercise was largely successful.  I was impressed with the output that I recieved from the models.  The models greatly accelerated the design, build and test processes.  
It was a collaborative effort as the design decisions suggested by the models needed to be vetted and sometimes refined.  The models also helped bridge many of the knowledge gaps that serve as obstacles to quickly completing such a project, especially when performing the exercise solo. 

# Phase 2 - Incorporated Azure Data Factory as the ETL layer, utilizing a medallion architecture in Azure SQL DB to load the data to Bronze, Silver and Gold layer data structures.
Built a new version of the Power BI dashboard which utilizes DirectQuery to source data for the reports from medallion Gold layer views in Azure SQL DB.  Used AI to define the approach to the solution, upskill on Azure Data Factory and troubleshoot the solution's implementation in Azure and Power BI.  

As this project was implemented in Power BI dashboard with a DirectQuery interface to Azure SQL DB, the result cannot be shared via an upload of the PowerBI file. Instead, I have created a project overview PDF with the DDL, Stored Procedures, DAX measures, data models, report snapshots and learnings:  "AI Assisted Azure Data Factory Power BI Exercise .pdf"
