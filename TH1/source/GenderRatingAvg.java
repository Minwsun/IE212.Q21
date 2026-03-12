import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderRatingAvg {

    // Mapper: doc ratings, join voi users (tu cache), emit (MovieID_Gender, Rating)
    public static class GenderMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        // Map: UserID -> Gender
        private Map<String, String> userGender = new HashMap<>();
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fs = FileSystem.get(context.getConfiguration());
                for (URI cacheFile : cacheFiles) {
                    Path path = new Path(cacheFile.toString());
                    BufferedReader reader = new BufferedReader(
                            new InputStreamReader(fs.open(path)));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) continue;
                        // Format: UserID, Gender, Age, Occupation, Zip-code
                        String[] parts = line.split(",\\s*");
                        if (parts.length >= 2) {
                            String userId = parts[0].trim();
                            String gender = parts[1].trim();
                            userGender.put(userId, gender);
                        }
                    }
                    reader.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Format: UserID, MovieID, Rating, Timestamp
            String[] parts = line.split(",\\s*");
            if (parts.length >= 3) {
                String userId = parts[0].trim();
                String movieId = parts[1].trim();
                String ratingVal = parts[2].trim();

                String gender = userGender.get(userId);
                if (gender != null) {
                    // Key: MovieID_Gender (vd: "1043_F")
                    outputKey.set(movieId + "\t" + gender);
                    outputValue.set(ratingVal);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    // Reducer: tinh trung binh va dem
    public static class GenderReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;

            for (Text val : values) {
                sum += Double.parseDouble(val.toString());
                count++;
            }

            double avg = sum / count;
            String result = count + "\t" + String.format("%.2f", avg);
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        // args[0] = input ratings path
        // args[1] = output path
        // args[2] = users.txt path (tren HDFS)

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Average");
        job.setJarByClass(GenderRatingAvg.class);

        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
