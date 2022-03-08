# ukg-raas

### Read UKG Reports as a Service into Pandas DataFrames

This code was created for the purpose of retrieving report-style data created in UKG's BI tool for use in any
python ETL pipelines. Reports accessible via UKG's Reports as a Service (RaaS) can 
be returned in a Pandas DataFrame.

After adding your credentials to the `create_client()` function, gather the path/folder structure of 
the report in UKG and pass it to the `get_ukg_report()` function to get the data as a dataframe. An example is 
to call `df = get_ukg_report(report_path="/path/to/your/report")`.

Reports designed in UKG for this purpose should be strictly rows and columns, where the first row is a header, and 
no cells are merged. Avoid adding columns with special characters into the report. The data is initially 
received via this pipeline as a single long string and must be parsed into rows by patterns of 
special characters, and the precense of additional special characters in the data itself may
cause rows to be split into two or more rows, or data to be misaligned across columns.

This work was based on this 
[ultipro-soap-python](https://github.com/puppetlabs/ultipro-soap-python/tree/243c1185cbbd80a8c21c2c61f5a8b8302f45eebd) 
repository, which provided the authentication and execute/retrieve report functionality. That repository 
was designed to be able to download reports from UKG via terminal commands. This repository builds 
on that, providing a function that can gather a report in-memory as a Pandas DataFrame. This allows for 
further manipulation of the data for use in any ETL processes. Additionally, as no physical excel
files are used in gathering the data, this code is more straightforward to use cloud environments.

