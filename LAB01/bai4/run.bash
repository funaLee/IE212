cd ~/UIT/IE212/LAB01/bai4
rm -rf build && mkdir build
javac -classpath "$(hadoop classpath)" -d build bai4.java
jar -cvf bai4.jar -C build .

# làm sạch & nạp lại input
hdfs dfs -rm -r -f /user/$USER/input /user/$USER/output_bai4
hdfs dfs -mkdir -p /user/$USER/input

hdfs dfs -put -f ratings_1.txt /user/$USER/input
hdfs dfs -put -f ratings_2.txt /user/$USER/input
hdfs dfs -put -f movies.txt /user/$USER/movies.txt
hdfs dfs -put -f users.txt  /user/$USER/users.txt

# chạy (FQCN do có package bai4;)
hadoop jar bai4.jar bai4.bai4 \
  /user/$USER/input \
  /user/$USER/output_bai4 \
  /user/$USER/movies.txt \
  /user/$USER/users.txt

# xem kết quả
hdfs dfs -cat /user/$USER/output_bai4/part-r-00000
