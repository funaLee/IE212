cd ~/UIT/IE212/LAB01/bai1
rm -rf build && mkdir build
javac -classpath "$(hadoop classpath)" -d build bai1.java
jar -cvf bai1.jar -C build .

# làm sạch & nạp lại input
hdfs dfs -rm -r -f /user/$USER/input /user/$USER/output_bai1
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -put -f ratings_1.txt /user/$USER/input
hdfs dfs -put -f ratings_2.txt /user/$USER/input
hdfs dfs -put -f movies.txt /user/$USER/   # đúng với tham số 3 phía dưới

# chạy (đúng FQCN do có package bai1;)
hadoop jar bai1.jar bai1.bai1 /user/$USER/input /user/$USER/output_bai1 /user/$USER/movies.txt

# xem kết quả
hdfs dfs -cat /user/$USER/output_bai1/part-r-00000
