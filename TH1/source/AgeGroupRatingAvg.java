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

public class AgeGroupRatingAvg {

    // Phan loai nhom tuoi
    public static String getAgeGroup(int age) {
        if (age <= 18) return "0-18";
        else if (age <= 35) return "18-35";
        else if (age <= 50) return "35-50";
        else return "50+";
    }

    // Mapper
    public static class AgeGroupMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        // Map: UserID -> Age
        private Map<String, Integer> userAge = new HashMap<>();
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
                        if (parts.length >= 3) {
                            String userId = parts[0].trim();
                            int age = Integer.parseInt(parts[2].trim());
                            userAge.put(userId, age);
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

                Integer age = userAge.get(userId);
                if (age != null) {
                    String ageGroup = getAgeGroup(age);
                    // Key: MovieID_AgeGroup
                    outputKey.set(movieId + "\t" + ageGroup);
                    outputValue.set(ratingVal);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    // Reducer
    public static class AgeGroupReducer
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
        Job job = Job.getInstance(conf, "AgeGroup Rating Average");
        job.setJarByClass(AgeGroupRatingAvg.class);

        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
