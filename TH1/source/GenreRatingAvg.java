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

public class GenreRatingAvg {

    // Mapper: doc ratings, join voi movies (tu cache), emit (Genre, Rating)
    public static class GenreMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        // Map: MovieID -> Genres (vd: "Animation|Children|Comedy")
        private Map<String, String> movieGenres = new HashMap<>();
        private Text genre = new Text();
        private Text rating = new Text();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            // Doc file movies.txt tu Distributed Cache
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
                        // Format: MovieID, Title, Genres
                        String[] parts = line.split(",\\s*", 3);
                        if (parts.length >= 3) {
                            String movieId = parts[0].trim();
                            String genres = parts[2].trim();
                            movieGenres.put(movieId, genres);
                        }
                    }
                    reader.close();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // Format: UserID, MovieID, Rating, Timestamp
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",\\s*");
            if (parts.length >= 3) {
                String movieId = parts[1].trim();
                String ratingVal = parts[2].trim();

                String genres = movieGenres.get(movieId);
                if (genres != null) {
                    // Tach cac genre (phan cach boi "|")
                    String[] genreList = genres.split("\\|");
                    for (String g : genreList) {
                        genre.set(g.trim());
                        rating.set(ratingVal);
                        context.write(genre, rating);
                    }
                }
            }
        }
    }

    // Reducer: tinh trung binh va dem so luot danh gia theo genre
    public static class GenreReducer
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
        // args[2] = movies.txt path (tren HDFS)

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Average");
        job.setJarByClass(GenreRatingAvg.class);

        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Them movies.txt vao Distributed Cache
        job.addCacheFile(new URI(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
