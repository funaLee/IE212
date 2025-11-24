package bai3;

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

public class bai3 {

    // chấp nhận :: hoặc ", *" hoặc "; *" hoặc tab
    private static final Pattern DELIM = Pattern.compile("::|,\\s*|;\\s*|\\t");

    // ───────────────────────────
    // Mapper: (ratings) → (MovieID, "M:4.0") hoặc "F:3.5"
    // Nạp users + movies bằng Distributed Cache (map-side join)
    public static class JoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, Character> userGender = new HashMap<>();
        private final Map<Integer, String> movieTitle = new HashMap<>();
        private final IntWritable outKey = new IntWritable();
        private final Text outVal = new Text();

        @Override
        protected void setup(Context context) throws IOException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                Path p = new Path(uri);
                File f = new File(p.getName());
                if (!f.exists()) f = new File(p.toString());

                String lower = f.getName().toLowerCase();
                if (lower.contains("users")) {
                    // users.txt: UserID, Gender, Age, Occupation, Zip
                    try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            line = line.trim();
                            if (line.isEmpty()) continue;
                            if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                            String[] parts = DELIM.split(line, -1);
                            if (parts.length >= 2) {
                                try {
                                    int uid = Integer.parseInt(parts[0].trim());
                                    String g = parts[1].trim();
                                    if (!g.isEmpty()) {
                                        char G = Character.toUpperCase(g.charAt(0));
                                        if (G == 'M' || G == 'F') userGender.put(uid, G);
                                    }
                                } catch (NumberFormatException ignore) {}
                            }
                        }
                    }
                } else if (lower.contains("movie")) {
                    // movies.txt: MovieID, Title, Genres
                    try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            line = line.trim();
                            if (line.isEmpty()) continue;
                            if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                            String[] parts = DELIM.split(line, -1);
                            if (parts.length >= 2) {
                                try {
                                    int mid = Integer.parseInt(parts[0].trim());
                                    String title = parts[1].trim();
                                    movieTitle.put(mid, title);
                                } catch (NumberFormatException ignore) {}
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context ctx)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            if (line.charAt(0) == '\uFEFF') line = line.substring(1);
            // bỏ header
            char c0 = line.charAt(0);
            if (!Character.isDigit(c0) && c0 != '-') return;

            // ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = DELIM.split(line, -1);
            if (parts.length < 3) return;

            try {
                int userId = Integer.parseInt(parts[0].trim());
                int movieId = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                Character g = userGender.get(userId);
                if (g == null) { ctx.getCounter("Bai3", "no_gender_for_user").increment(1); return; }

                // gắn trước tiêu đề để reducer in đẹp (nếu không có vẫn ok)
                // không cần gửi title trong value; reducer sẽ tra lại theo movieId khi in.
                outKey.set(movieId);
                outVal.set(g + ":" + rating);
                ctx.write(outKey, outVal);
                ctx.getCounter("Bai3", "mapped_ok").increment(1);

            } catch (NumberFormatException e) {
                ctx.getCounter("Bai3", "bad_number").increment(1);
            }
        }
    }

    // ───────────────────────────
    // Reducer: (MovieID, ["M:4.0","F:3.5",...]) → "Title\tMale: x.x, Female: y.y"
    public static class GenderAvgReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private final Map<Integer, String> movieTitle = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            // nạp lại movies.txt để in tiêu đề
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                Path p = new Path(uri);
                File f = new File(p.getName());
                if (!f.exists()) f = new File(p.toString());
                if (!f.getName().toLowerCase().contains("movie")) continue;

                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                        String[] parts = DELIM.split(line, -1);
                        if (parts.length >= 2) {
                            try {
                                int mid = Integer.parseInt(parts[0].trim());
                                movieTitle.put(mid, parts[1].trim());
                            } catch (NumberFormatException ignore) {}
                        }
                    }
                }
            }
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> vals, Context ctx)
                throws IOException, InterruptedException {
            double mSum = 0, fSum = 0;
            int mCnt = 0, fCnt = 0;

            for (Text t : vals) {
                String s = t.toString();
                int colon = s.indexOf(':');
                if (colon <= 0) continue;
                char g = s.charAt(0);
                double r = Double.parseDouble(s.substring(colon + 1));
                if (g == 'M') { mSum += r; mCnt++; }
                else if (g == 'F') { fSum += r; fCnt++; }
            }

            double mAvg = (mCnt == 0) ? 0.0 : (mSum / mCnt);
            double fAvg = (fCnt == 0) ? 0.0 : (fSum / fCnt);

            String title = movieTitle.getOrDefault(key.get(), "MovieID " + key.get());
            String out = String.format(Locale.US,
                    "%s\tMale: %.2f, Female: %.2f", title, mAvg, fAvg);
            ctx.write(new Text(out), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai3 <ratings_input_dir> <output_dir> <movies.txt_path> <users.txt_path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender-based Movie Rating Averages");
        job.setJarByClass(bai3.class);

        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(GenderAvgReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // cache: movies + users
        job.addCacheFile(new URI(args[2] + "#movies.txt"));
        job.addCacheFile(new URI(args[3] + "#users.txt"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
