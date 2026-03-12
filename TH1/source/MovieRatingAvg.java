import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingAvg {

    // Mapper: doc tung dong rating, emit (MovieID, Rating)
    public static class RatingMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private Text movieId = new Text();
        private Text rating = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Format: UserID, MovieID, Rating, Timestamp
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");
            if (parts.length >= 3) {
                movieId.set(parts[1].trim());
                rating.set(parts[2].trim());
                context.write(movieId, rating);
            }
        }
    }

    // Reducer: tinh trung binh va dem so luot danh gia
    public static class RatingReducer
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Average");
        job.setJarByClass(MovieRatingAvg.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
