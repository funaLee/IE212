package bai2;

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

public class bai2 {

    // Cho phép nhiều loại delimiter: "::" hoặc ", *" hoặc "; *" hoặc tab
    private static final Pattern DELIM = Pattern.compile("::|,\\s*|;\\s*|\\t");

    // Mapper: (UserId,MovieId,Rating,Timestamp) -> (Genre, Rating)
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Map<Integer, List<String>> movieGenres = new HashMap<>();
        private final Text outKey = new Text();
        private final DoubleWritable outVal = new DoubleWritable();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                Path p = new Path(uri);
                File f = new File(p.getName());
                if (!f.exists()) f = new File(p.toString());
                if (!f.getName().toLowerCase().contains("movies")) continue;

                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        if (line.charAt(0) == '\uFEFF') line = line.substring(1);

                        // movies: MovieID, Title, Genres  (Genres split by "|")
                        String[] parts = DELIM.split(line, -1);
                        if (parts.length >= 3) {
                            try {
                                int movieId = Integer.parseInt(parts[0].trim());
                                String genresField = parts[parts.length - 1].trim(); // cột cuối là Genres
                                if (!genresField.isEmpty()) {
                                    String[] gs = genresField.split("\\|");
                                    List<String> list = new ArrayList<>();
                                    for (String g : gs) {
                                        g = g.trim();
                                        if (!g.isEmpty() && !g.equalsIgnoreCase("(no genres listed)")) {
                                            list.add(g);
                                        }
                                    }
                                    if (!list.isEmpty()) movieGenres.put(movieId, list);
                                }
                            } catch (NumberFormatException ignore) { /* skip bad line */ }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.charAt(0) == '\uFEFF') line = line.substring(1);
            // bỏ header nếu có
            char c0 = line.charAt(0);
            if (!Character.isDigit(c0) && c0 != '-') return;

            // ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = DELIM.split(line, -1);
            if (parts.length >= 3) {
                try {
                    int movieId = Integer.parseInt(parts[1].trim());
                    double rating = Double.parseDouble(parts[2].trim());
                    List<String> gs = movieGenres.get(movieId);
                    if (gs != null) {
                        outVal.set(rating);
                        for (String g : gs) {
                            outKey.set(g);
                            context.write(outKey, outVal);
                        }
                        context.getCounter("Bai2", "mapped_ok").increment(1);
                    } else {
                        context.getCounter("Bai2", "movie_without_genre").increment(1);
                    }
                } catch (NumberFormatException e) {
                    context.getCounter("Bai2", "mapped_bad_number").increment(1);
                }
            } else {
                context.getCounter("Bai2", "mapped_too_few_fields").increment(1);
            }
        }
    }

    // Reducer: (Genre, list<Rating>) -> "Genre\tAvg: x.xx, Count: n"
    public static class GenreReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> vals, Context ctx)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int cnt = 0;
            for (DoubleWritable v : vals) {
                sum += v.get();
                cnt++;
            }
            if (cnt == 0) return;
            double avg = sum / cnt;
            String out = String.format(Locale.US, "%-12s Avg: %.2f, Count: %d", key.toString(), avg, cnt);
            ctx.write(new Text(out), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: Bai2 <ratings_input_dir> <output_dir> <movies.txt_path>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Average & Count");
        job.setJarByClass(bai2.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.addCacheFile(new URI(args[2] + "#movies.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
