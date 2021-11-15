import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.net.URISyntaxException;

public class RecommendMovie {

    public static void main(String[] args) throws URISyntaxException {
        SparkSession spark = SparkSession.builder()
                .appName("MovieRecommendationApp")
                .master("local[*]")
                .getOrCreate();

        RecommendMovie main = new RecommendMovie();
        Dataset<Row> movieSimilarity = main.getMovieSimilarity(spark);
        int targetMovie = Integer.parseInt(args[0]);
        double scoreThreshold = Double.parseDouble(args[1]);
        int pairThreshold = Integer.parseInt(args[2]);
        main.findMostSimilarMovies(spark, movieSimilarity, targetMovie, scoreThreshold, pairThreshold);
    }

    public Dataset<Row> getMovieSimilarity(SparkSession spark) throws URISyntaxException {
        String path = "movie_relationship";
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .csv(path)
                .toDF("movie1", "movie2", "num_pairs", "similarity");
        return df;
    }

    public void findMostSimilarMovies(SparkSession spark, Dataset<Row> movieSimilarities, int targetMovie,
                                      double scoreThreshold, int pairThreshold) throws URISyntaxException {
        String path = getClass().getClassLoader().getResource("movie_names.csv").toURI().getPath();
        Dataset<Row> movie_names = spark
                .read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .csv(path)
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
