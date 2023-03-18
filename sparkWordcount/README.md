# sparkWordcount
sparkWordcount example


http://www.javaworld.com/article/2972863/big-data/open-source-java-projects-apache-spark.html

mvn clean package

spark-submit --class com.test.WordCount --master local SparkScala-0.0.1-SNAPSHOT.jar /user/sdhandap/input/file.txt

spark-submit --class com.geekcap.javaworld.sparkexample.WordCount --master local SparkScala-0.0.1-SNAPSHOT.jar /user/sdhandap/input/file.txt
