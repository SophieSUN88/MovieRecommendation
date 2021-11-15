import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URISyntaxException;

public class RecommendMovieAWS {

    public static void main(String[] args) throws URISyntaxException {
        SparkSession spark = SparkSession.builder()
                .appName("MovieRecommendationApp")
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", " ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", " ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl"," ");
        spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.aws.credentials.provider"," ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", " ");

        RecommendMovieAWS main = new RecommendMovieAWS();
        Dataset<Row> movieSimilarity = main.getMovieSimilarity(spark);
        int targetMovie = Integer.parseInt(args[0]);
        double scoreThreshold = Double.parseDouble(args[1]);
        int pairThreshold = Integer.parseInt(args[2]);
        main.findMostSimilarMovies(spark, movieSimilarity, targetMovie, scoreThreshold, pairThreshold);
    }

    public Dataset<Row> getMovieSimilarity(SparkSession spark){
        String path = "s3: ";
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .csv(path)
                .toDF("movie1", "movie2", "num_pairs", "similarity");
        return df;
    }

    public void findMostSimilarMovies(SparkSession spark, Dataset<Row> movieSimilarities, int targetMovie,
                                      double scoreThreshold, int pairThreshold) throws URISyntaxException {
        Dataset<Row> movie_names = spark
                .read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .csv("s3:// ")
                .toDF()
                .select("id", "original_title");

        Dataset<String> movieList = movieSimilarities
                .where(String.format("num_pairs > %s", pairThreshold))
                .where(String.format("similarity > %s", scoreThreshold))
                .where(movieSimilarities.col("movie1").equalTo(targetMovie)
                        .or(movieSimilarities.col("movie2").equalTo(targetMovie)))
                .sort(movieSimilarities.col("similarity").desc())
                .limit(5)
                .map((MapFunction<Row, String>) row -> {
                    String movie1 = row.getAs("movie1");
                    String movie2 = row.getAs("movie2");
                    return movie1.equalsIgnoreCase(String.valueOf(targetMovie))? movie2: movie1;
                }, Encoders.STRING());

        Dataset<Row> recommendedMovieNames = movieList
                .join(movie_names, movieList.col("value").equalTo(movie_names.col("id")), "left")
                .select(movie_names.col("original_title"));

        recommendedMovieNames.show();
    }
}
