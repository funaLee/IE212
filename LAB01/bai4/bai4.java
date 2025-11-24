package bai4;

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

public class bai4 {

    // Chấp nhận ::, CSV (,), ;, tab (kèm khoảng trắng)
    private static final Pattern DELIM = Pattern.compile("::|,\\s*|;\\s*|\\t");

    // ───────── helpers ─────────
    // Trả về index nhóm tuổi: 0=0-18, 1=18-35, 2=35-50, 3=50+
    private static int ageGroup(int age) {
        if (age < 18) return 0;
        if (age < 35) return 1;
        if (age < 50) return 2;
        return 3;
    }

    // ───────── Mapper ─────────
    // Đầu ra: (MovieID, "Gk:rating")  k ∈ {0,1,2,3}
    public static class AgeMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final Map<Integer, Integer> userAge = new HashMap<>();     // userId -> age
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
                if (!f.getName().toLowerCase().contains("users")) continue;

                try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        if (line.charAt(0) == '\uFEFF') line = line.substring(1);
                        // users: UserID, Gender, Age, Occupation, Zip
                        String[] parts = DELIM.split(line, -1);
                        if (parts.length >= 3) {
                            try {
                                int uid = Integer.parseInt(parts[0].trim());
                                int age = Integer.parseInt(parts[2].trim());
                                userAge.put(uid, age);
                            } catch (NumberFormatException ignore) {}
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
            char c0 = line.charAt(0);
            if (!Character.isDigit(c0) && c0 != '-') return;

            // ratings: UserID, MovieID, Rating, Timestamp
            String[] parts = DELIM.split(line, -1);
            if (parts.length < 3) return;

            try {
                int uid = Integer.parseInt(parts[0].trim());
                int mid = Integer.parseInt(parts[1].trim());
                double rating = Double.parseDouble(parts[2].trim());

                Integer age = userAge.get(uid);
                if (age == null) { ctx.getCounter("Bai4", "no_age").increment(1); return; }

                int g = ageGroup(age);
                outKey.set(mid);
                outVal.set("G" + g + ":" + rating);
                ctx.write(outKey, outVal);
                ctx.getCounter("Bai4", "mapped_ok").increment(1);
            } catch (NumberFormatException e) {
                ctx.getCounter("Bai4", "bad_number").increment(1);
            }
        }
    }

    // ───────── Reducer ─────────
    // Gom theo phim, tính trung bình cho 4 nhóm tuổi. In NA nếu nhóm không có dữ liệu.
    public static class AgeReducer extends Reducer<IntWritable, Text, Text, NullWritable> {
        private final Map<Integer, String> movieTitle = new HashMap<>();
        private static final String[] LABELS = {"0-18", "18-35", "35-50", "50+"};

        @Override
        protected void setup(Context context) throws IOException {
            // nạp movies.txt để in tiêu đề
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
                        // movies: MovieID, Title, Genres
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

            double[] sum = new double[4];
            int[] cnt = new int[4];

            for (Text t : vals) {
                String s = t.toString();      // "Gk:rating"
                int c = s.indexOf(':');
                if (c <= 1) continue;
                int g = s.charAt(1) - '0';
                if (g < 0 || g > 3) continue;
                double r = Double.parseDouble(s.substring(c + 1));
                sum[g] += r;
                cnt[g] += 1;
            }

            String title = movieTitle.getOrDefault(key.get(), "MovieID " + key.get());
            StringBuilder sb = new StringBuilder();
            sb.append(title).append("\t");
            for (int i = 0; i < 4; i++) {
                if (i > 0) sb.append("  ");
                if (cnt[i] == 0) {
                    sb.append(String.format("%s: NA", LABELS[i]));
                } else {
                    double avg = sum[i] / cnt[i];
                    sb.append(String.format(Locale.US, "%s: %.2f", LABELS[i], avg));
                }
            }
            ctx.write(new Text(sb.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: Bai4 <ratings_input_dir> <output_dir> <movies.txt_path> <users.txt_path>");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Averages per Movie");
        job.setJarByClass(bai4.class);

        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

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
