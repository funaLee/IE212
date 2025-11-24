package bai1;
import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    // Tách theo :: hoặc ", *" hoặc "; *" hoặc tab
    private static final Pattern DELIM = Pattern.compile("::|,\\s*|;\\s*|\\t");

    // ====== Mapper: đọc ratings (CSV hoặc ::) ======
    // Dòng: UserID, MovieID, Rating, Timestamp  (hoặc  UserID::MovieID::Rating::Timestamp)
    public static class RatingMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
        private final IntWritable movieIdKey = new IntWritable();
        private final DoubleWritable ratingValue = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Bỏ BOM nếu có
            if (!line.isEmpty() && line.charAt(0) == '\uFEFF') {
                line = line.substring(1);
            }
            // Bỏ header nếu có (nếu dòng không bắt đầu bằng số)
            char c0 = line.charAt(0);
            if (!Character.isDigit(c0) && c0 != '-') return;

            String[] parts = DELIM.split(line);
            if (parts.length >= 3) {
                try {
                    int movieId = Integer.parseInt(parts[1].trim());
                    double rating = Double.parseDouble(parts[2].trim());
                    movieIdKey.set(movieId);
                    ratingValue.set(rating);
                    context.write(movieIdKey, ratingValue);
                    context.getCounter("Bai1", "mapped_ok").increment(1);
                } catch (NumberFormatException e) {
                    context.getCounter("Bai1", "mapped_bad_number").increment(1);
                }
            } else {
                context.getCounter("Bai1", "mapped_too_few_fields").increment(1);
            }
        }
    }

    // ====== Reducer: tính trung bình + đếm, in tiêu đề phim (nếu có) ======
    public static class RatingReducer extends Reducer<IntWritable, DoubleWritable, Text, NullWritable> {
        private final Map<Integer, String> movieTitles = new HashMap<>();
        private int bestMovieId = -1;
        private double bestAvg = -1.0;

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {
                    Path p = new Path(uri);
                    File f = new File(p.getName());
                    if (!f.exists()) f = new File(p.toString());
                    if (f.getName().toLowerCase().contains("movies")) {
                        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                            String line;
                            while ((line = br.readLine()) != null) {
                                if (line.isEmpty()) continue;
                                if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                                String[] parts = DELIM.split(line); // dùng cùng delimiter
                                if (parts.length >= 2) {
                                    try {
                                        int movieId = Integer.parseInt(parts[0].trim());
                                        String title = parts[1].trim();
                                        movieTitles.put(movieId, title);
                                    } catch (NumberFormatException ignore) {}
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            for (DoubleWritable v : values) {
                sum += v.get();
                count++;
            }
            if (count == 0) return;

            double avg = sum / count;
            String title = movieTitles.getOrDefault(key.get(), "MovieID " + key.get());

            String line = String.format(Locale.US,
                    "%s\tAverage rating: %.1f (Total ratings: %d)",
                    title, avg, count);
            context.write(new Text(line), NullWritable.get());

            if (count >= 5 && avg > bestAvg) {
                bestAvg = avg;
                bestMovieId = key.get();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (bestMovieId != -1) {
                String bestTitle = movieTitles.getOrDefault(bestMovieId, "MovieID " + bestMovieId);
                String summary = String.format(Locale.US,
                        "%s is the highest rated movie with an average rating of %.1f among movies with at least 5 ratings.",
                        bestTitle, bestAvg);
                context.write(new Text(summary), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai1 <ratings_input_dir_or_files> <output_dir> <movies.txt_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Average & Count (CSV-friendly)");
        job.setJarByClass(bai1.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
