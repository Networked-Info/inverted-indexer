#Changelog:

March 18 - TCW
- changed mapper/reducer names to reflect ops
- refactored driver - broke each job config prep into methods
- changed args to include filename for taking sample for wordcount
- added loop to kill driver once runner thread is done
- set stopword threshold to 15,000
- implemented processing stopwords/adding set/ passing to IndexMapper job using GSON
- tested chaining
- ran on cluster

- TODO: Cluster output includes unusual characters
