from pyspark import SparkContext
import time

#This function spearate and abstract a single researcher from a group of researchers
def pair_sample_to_researcher(record):
  try:
    sample, C02, C03, C04, C05, C06, C07, researcherName = record.strip().split(",")
    reseachers = researcherName.strip().split(";")
    return [(sample, (reseacher.strip())) for reseacher in reseachers]
  except:
    return []

def extract_sample(line):
  sample,FSC_A,SSC_A,C04,C05,C06,C07,C08,C09,C10,C11,C12,C13,C14,C15,C16,C17 = line.strip().split(",")
  return (sample,FSC_A,SSC_A)

def filter_sample(line):
  sample,FSC_A,SSC_A = line
  return (1 <= int(FSC_A) and int(FSC_A) <= 150000) and (1 <= int(SSC_A) and int(SSC_A) <= 150000)

def setOne(line):
  sample,FSC_A,SSC_A = line
  return (sample.strip(), 1)

# This function is used by reduceByKey function to merge count of the same key
# This functions takes in two values - merged count from previous call of sum_rating_count, and the currently processed count
def sum_sample_count(reduced_count, current_count):
  return reduced_count+current_count

def write(line):
  researcher_name, measurement_data = line
  return ("{}\t{}".format(researcher_name, measurement_data))


# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
# Thus the code under the if statement will only be evaluated when the script is called from command line
if __name__ == "__main__":

  start_time = time.time()

  
  sc = SparkContext(appName="Number of (valid) measurements conducted per researcher")
  resultsRaw = sc.textFile("/share/cytometry/large/measurements_arcsin200_p*.csv")
  #resultsRaw = sc.textFile("/share/cytometry/large/measurements_arcsin200_p1.csv")

  skipable_first_row = resultsRaw.first()
  results = resultsRaw.filter(lambda line: line != skipable_first_row) 
  
  experiments = sc.textFile("/share/cytometry/experiments.csv")
  skipable_first_row2 = experiments.first()
  experiments = experiments.filter(lambda line: line != skipable_first_row2) 
  
  resultsSub = results.map(extract_sample)

  resultsFilt = resultsSub.filter(filter_sample)
  resultsSetOne = resultsFilt.map(setOne)

  sample_count = resultsSetOne.reduceByKey(sum_sample_count)
  

  sample_researcher = experiments.flatMap(pair_sample_to_researcher)
  sample_info = sample_researcher.join(sample_count)
  
  sample_info_val = sample_info.values()
  researcher_exp_times = sample_info_val.reduceByKey(sum_sample_count).map(lambda x:(x[1], x[0])).sortByKey(ascending=False).map(lambda x:(x[1], x[0]))

  #write to file
  researcher_exp_times.map(write).repartition(1).saveAsTextFile("ass2_task1")
  '''
  for i in range(len(researcher_exp_times.collect())):
    print("{}\t{}".format(researcher_exp_times.collect()[i][0], researcher_exp_times.collect()[i][1])) 
  '''
  print("--- %s seconds ---" % (time.time() - start_time))
