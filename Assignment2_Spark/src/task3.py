from pyspark import SparkContext
from random import random
import time

# This function convert entries of input file into key,value pair of the following format
def extract_sample(line):
  sample,FSC_A,SSC_A,C04,C05,C06,SCA1,CD11b,C09,C10,C11,Ly6C,C13,C14,C15,C16,C17 = line.strip().split(",")
  return (sample, FSC_A, SSC_A, Ly6C, CD11b, SCA1)

def filter_sample(line):
  sample,FSC_A,SSC_A, Ly6C, CD11b, SCA1 = line
  return (1 <= int(FSC_A) and int(FSC_A) <= 150000) and (1 <= int(SSC_A) and int(SSC_A) <= 150000)


def k_means_start(record, input_centr):
  
  sample, FSC_A, SSC_A, Ly6C, CD11b, SCA1 = record
  sort_dis = {}

  for i in range(len(input_centr)/3):
    dis_squ = ( float(Ly6C) - input_centr[i*3] )**2 + ( float(CD11b) - input_centr[i*3+1] )**2 + ( float(SCA1) - input_centr[i*3+2] )**2
    sort_dis[i+1] = dis_squ
  
  res = sorted(dict((v,k) for k,v in sort_dis.iteritems()))
  rev_sort_dis = dict((v,k) for k,v in sort_dis.iteritems())
  
  return rev_sort_dis[res[0]], (float(Ly6C), float(CD11b), float(SCA1))


def clust_point_disMin(record, input_centr):
  
  sample, FSC_A, SSC_A, Ly6C, CD11b, SCA1 = record
  sort_dis = {}

  for i in range(len(input_centr)/3):
    dis_squ = ( float(Ly6C) - input_centr[i*3] )**2 + ( float(CD11b) - input_centr[i*3+1] )**2 + ( float(SCA1) - input_centr[i*3+2] )**2
    sort_dis[i+1] = dis_squ
  
  res = sorted(dict((v,k) for k,v in sort_dis.iteritems()))
  rev_sort_dis = dict((v,k) for k,v in sort_dis.iteritems())
  
  return rev_sort_dis[res[0]], (float(Ly6C), float(CD11b), float(SCA1), res[0])

# This function is used by the aggregateByKey function to merge centroid of the same keys in the same combiner
# This function takes in either the starting value (0.0,0.0,0.0,0) or the result of previous call to marge_centr function
# in the form of (total sum x, total sum y,total sum z,number of times), and the value currently being 'processed'.
def marge_centr(accumulated_pair, current_centr):
  centr_totalx, centr_totaly, centr_totalz, centr_count = accumulated_pair
  centr_totalx += current_centr[0]
  centr_totaly += current_centr[1]
  centr_totalz += current_centr[2]
  centr_count += 1
  return (centr_totalx, centr_totaly, centr_totalz, centr_count)

# This function is used by the aggregateByKey function to merge rating of the same keys from different combiner
# This function takes in the result of marge_rating function from different combiners (in the form of (total rating, number of rating))
def merge_combiners(accumulated_pair_1, accumulated_pair_2):
  centr_total_11, centr_total_12, centr_total_13, centr_count_1 = accumulated_pair_1
  centr_total_21, centr_total_22, centr_total_23, centr_count_2 = accumulated_pair_2
  return (centr_total_11+centr_total_21, centr_total_12+centr_total_22, centr_total_13+centr_total_23, centr_count_1+centr_count_2)

# This function takes the statistic of ratings per genre and calculate the average rating
def map_average_centr(line):
  cluster, centr_total_count = line
  centr_average_x = centr_total_count[0]/centr_total_count[3]
  centr_average_y = centr_total_count[1]/centr_total_count[3]
  centr_average_z = centr_total_count[2]/centr_total_count[3]
  return (cluster, (centr_average_x, centr_average_y, centr_average_z), centr_total_count[3])

def dis_centrs(line):
  cluster, centr_info = line
  return centr_info[3],(centr_info[0], centr_info[1], centr_info[2])

#inorder to reuse the fuctions defined before, need to restruct to coordinate the input
def organizeStruct4kmeans(line):
  distance, point = line
  return 1, 1, 1, point[0], point[1], point[2]
  

def write(line):
  #clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
  clusterID, centroid, number_of_measurements = line
  return ("{}\t{}\t{}\t{}\t{}" .format(clusterID, number_of_measurements, centroid[0], centroid[1], centroid[2]))
  
# This is line is important for submitting python jobs through spark-submit!
# The conditional __name__ == "__main__" will only evaluates to True in this script is called from command line (i.e. pythons <script.py>)
if __name__ == "__main__":

  #initiate interating times
  num_inters = 10
  #initiate clusters number
  clusters = 5

  #time test
  start_time = time.time()

  sc = SparkContext(appName="k-means clustering of the measurements")
  dataRaw = sc.textFile("/share/cytometry/large/measurements_arcsin200_p*.csv")
  #dataRaw = sc.textFile("/user/czha0172/mea_test_p*.csv")

  input_centr = []
  clus_centr = [[0]]*3*clusters
  index = []

  for i in range(clusters*3):
    input_centr.append(random())  

  skipable_first_row = dataRaw.first()
  data = dataRaw.filter(lambda line: line != skipable_first_row)

  resultSub = data.map(extract_sample)

  resultClear = resultSub.filter(filter_sample)

  for inters in range(num_inters):
    cluster_centr = resultClear.map(lambda i: k_means_start(i, input_centr))
    
    cluster_centr_sum = cluster_centr.aggregateByKey((0.0,0.0,0.0,0), marge_centr, merge_combiners, 1)
    cluster_centr_avg = cluster_centr_sum.map(map_average_centr)

    #clear cluster records in a list
    input_centr[:] = []

    #updating new centroids and store in a list
    for i in range(clusters):
      for j in range(3):
        input_centr.append(cluster_centr_avg.collect()[i][1][j])


  ##-------------------------remove outliners
  cluster_centr_dis = resultClear.map(lambda i: clust_point_disMin(i, input_centr))

  for i in range(clusters):
    cluster_centr_dis = cluster_centr_dis.filter(lambda line: line[0] == i+1)
    dis_centr = cluster_centr_dis.map(dis_centrs).sortByKey(ascending=True)
    dis_centr_filOutliner = sc.parallelize(dis_centr.take(int(dis_centr.count()*0.9)))
    if i == 0:
      collectRDD = dis_centr_filOutliner
    else:
      collectRDD = sc.union([collectRDD, dis_centr_filOutliner])

  collectRDD = collectRDD.map(organizeStruct4kmeans)

  ##--------------------------detect clusters without outliners
  for inters in range(num_inters):
    cluster_centr = collectRDD.map(lambda i: k_means_start(i, input_centr))
    
    cluster_centr_sum = cluster_centr.aggregateByKey((0.0,0.0,0.0,0), marge_centr, merge_combiners, 1)
    cluster_centr_avg = cluster_centr_sum.map(map_average_centr)

    #clear cluster records in a list
    input_centr[:] = []

    #updating new centroids and store in a list
    for i in range(clusters):
      for j in range(3):
        input_centr.append(cluster_centr_avg.collect()[i][1][j])    


  #print final result
  
  cluster_centr_avg.map(write).repartition(1).saveAsTextFile("ass2_task3")
  '''    
  for i in range(clusters):
    #clusterID \t number_of_measurements \t Ly6C \t CD11b \t SCA1
    print("{}\t{}\t{}\t{}\t{}" .format(cluster_centr_avg.collect()[i][0], cluster_centr_avg.collect()[i][2], cluster_centr_avg.collect()[i][1][0], cluster_centr_avg.collect()[i][1][1], cluster_centr_avg.collect()[i][1][2]))
  '''
  print("--- %s seconds ---" % (time.time() - start_time))


 
