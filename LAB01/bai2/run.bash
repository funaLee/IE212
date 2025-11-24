cd ~/UIT/IE212/LAB01/bai2
rm -rf build && mkdir build
javac -classpath "$(hadoop classpath)" -d build bai2.java
jar -cvf bai2.jar -C build .

# làm sạch & nạp lại input
hdfs dfs -rm -r -f /user/$USER/input /user/$USER/output_bai2
hdfs dfs -mkdir -p /user/$USER/input
hdfs dfs -put -f ratings_1.txt /user/$USER/input
hdfs dfs -put -f ratings_2.txt /user/$USER/input
hdfs dfs -put -f movies.txt /user/$USER/   # đúng với tham số 3 phía dưới

hadoop jar bai2.jar bai2.bai2 /user/$USER/input /user/$USER/output_bai2 /user/$USER/movies.txt

# xem kết quả
hdfs dfs -cat /user/$USER/output_bai2/part-r-00000
