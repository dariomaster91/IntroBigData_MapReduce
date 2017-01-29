Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.4.0.jar")

hdfs.root <- 'exercise1'
hdfs.data <- file.path(hdfs.root, 'input', 'esempio.txt')
hdfs.out <- file.path(hdfs.root, 'out')

library(rmr2)

if(rmr2::dfs.exists("/user/hduser/exercise1/out")){
  rmr2::dfs.rmr("/user/hduser/exercise1/out")
}

rmr.options(hdfs.tempdir = "/tmp")
#rmr.options(backend = "local")

map <- function(k, input) {
  chiavi <- NULL
  lines <- strsplit(input, '\\n')
  for(line in lines) {
    month <- substr(line, 1, 7)
    products <- strsplit(substr(line, 12, nchar(line)), ",")
    for(product in products){
      key <- paste(month, product, sep='_')
      chiavi <- c(chiavi, key)
    }
  }
  keyval(chiavi, 1)
}

reduce <- function(month_product, counts){
  keyval(month_product, sum(counts))
}

exercise1 <- function(input, output){
  mapreduce(input=input, output=output, input.format = "text", output.format="text", map=map, reduce=reduce)
}

out <- exercise1(hdfs.data, hdfs.out)

results <- from.dfs('exercise1/out/part-00000')
results.df <- as.data.frame(results, stringsAsFactors = F)
colnames(results.df) <- c('month', 'count')
head(results.df[order(results.df$count, decreasing=T), ], 30)