# Set system env
Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.4.0.jar")

# Read input file from HDFS
library(rhdfs)
hdfs.init()
root <- '/introbigdata/'
input <- paste(root,"esempio.txt", sep="")
reader <- hdfs.line.reader(input)
x <- reader$read()

# Get max columns number and create the table
input.file <- file("./data/tmp.txt")
no_col <- max(count.fields(input.file, sep = ","))
data <- read.table(input.file, na.strings=c("", "NA"), sep = ",", fill = TRUE, col.names = 1:no_col)

# Extract year-month values
data$X1 <- substr(data$X1, 1, 7)

# Split rows in order to obtain a dataset with a date for each product purchesed
library(reshape2)
melteddata <- melt(data, id = "X1")

# obtain data frame from the table
df <- as.data.frame(melteddata)

# remove rows having NA values
df <- df[complete.cases(df),]

# assign the value one to each row in the dataframe
names(df) <- c("date", "variable", "product")

# remove useless column df$variable
df$variable <- NULL

# Create keys date_product
df["key"] <- paste(df$date, df$product, sep="_")

tmpfile <- hdfs.file(paste(root,"tmp.txt", sep=""),"w")
hdfs.write(df$key, tmpfile)

map <- function(k, input) {
  keyval(input, 1)
}

reduce <- function(month_product, counts){
  keyval(month_product, sum(counts))
}

exercise1 <- function(input, output){
  mapreduce(input=input, output=output, input.format = "text", output.format="text", map=map, reduce=reduce)
}

out <- exercise1(tmpfile, hdfs.out)
