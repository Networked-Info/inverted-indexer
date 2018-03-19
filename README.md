:gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem: :gem:

# CS132g1 -> Inverted Index Project
## TEAM NAME :star: "Team IPS Attack"
## Member :book:
Thomas Willkens | Bruce Chen | Si Chen | Phoebe Zhang

# Inverted Index :computer:

## 2.2 - Scrubwords/Stopwords

### Inverted Index Size Comparison 
(on sample size of three wiki csv files)

Basic Inverted Index: 125 MB
Scrub/Stopwords Removed: 85.9 MB

### Map/Reduce Job Flow:
WordCountMapper -> WordCountReducer -> StopwordMapper -> StopwordReducer -> IndexMapper -> IndexReducer

Job are chained using JobControl.

1. Wordcount job scans a sample wiki record to find and output word counts
2. Stopword job reads from Wordcount output and screens out words below 15,000 threshold
3. Index job takes in a set generated from Stopword job output and uses it to remove stopwords. A regex removes scrubwords

## Changelog
March 18
- changed mapper/reducer names to reflect ops
- refactored driver - broke each job config prep into methods
- changed args to include filename for taking sample for wordcount
- added loop to kill driver once runner thread is done
- set stopword threshold to 15,000
- implemented processing stopwords/adding set/ passing to IndexMapper job using GSON
- tested chaining
- ran on cluster

- TODO: Still need to remove "line separator" unicode character
