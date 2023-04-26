Stock ETL Project

For this project, I designed and implemented a PostgreSQL database hosted on my local machine to store data about US stocks. I also created a python script to manually scrape financial data from Yahoo into my database for analysis purposes. 
The second half of the project which will be continually iterated over is analysis of this data. The purpose of the analysis is Signal Generation so one can pin point when to buy or sell a stock. Additionally, I will develop a system for fund allocations. 
ETL is the general procedure of copying data from sources into a destination system that represents the data differently from the source(s). Data extraction involves extracting data, transformation by cleaning and transforming into a proper storage format for purposes of querying and analysis, finally data loading into a final the final target database such as a operational data store, a data mart

2 Motivation

My primary motivation for this project was to practice using the tools listed below, after spending time on smaller bite sized projects I felt ready enough to take on something slightly bigger that combines the use of all the tools listed below.

3 Skills and Tools

3.1 Skills
Database design - postgreSQL
data cleaning – pandas 
software development – postgresql, Docker, Git & Github(communication between different tools used)
version control – Git
project documentation 

4  Database Design Process

4.1  Requirements Specification
I started by specifying the requirements for my database.
The database must be able to:
•	store data about US stocks listed on Nasdaq
•	store price/volume data, fundamental data, and general identifying data for each stock
•	retroactively allow the addition of other types of financial instruments and fundamental data
•	easily join all data about a stock when queried
•	easily update to reflect new information
•	be queried with SQL and Python
•	run on a cloud computing platform such as AWS

