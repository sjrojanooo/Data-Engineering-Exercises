## Exercise 7 - Ingestion, Zipfiles, Streaming Data, and Parquet Files

This is individual work for me. I am continuing on with the material that I learned and applying some new concepts. 

#### Setup
1. Change directories at the command line 
   to be inside the `Exercise-7` folder `cd Exercises/Exercise-7`
   
2. Run `docker build --tag=exercise-7 .` to build the `Docker` image.

3. There is a file called `main.py` in the `Exercise-6` directory, this
is where you `Python` code to complete the exercise should go.
   
4. Once you have finished the project or want to test run your code,
   run the following command `docker-compose up run` from inside the `Exercises/Exercise-7` directory

### Problems Statement

1. Create a directory that will house all the data, I named it data. 
2. Request the endpoing and stream the zip file remotely, the zip file will not be saved to the project, only the csv files. 
3. compress files with pyspark and load them into an s3 bucket. 

