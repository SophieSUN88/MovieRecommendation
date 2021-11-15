import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

import java.net.URISyntaxException;

public class MovieRecommendationAWS {

    public static void main(String[] args) throws URISyntaxException {
        SparkSession spark = SparkSession.builder()
                .enableHiveSupport()
                .getOrCreate();
        MovieRecommendationAWS main = new MovieRecommendationAWS();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", " ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", " ");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl"," ");
        spark.sparkContext().hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", " ");

        //从原始csv数据创建user, movie, rating的dataframe
        Dataset<Row> userRating = main.getUserRating(spark, args[0]);

        //join userRating dataFrame来得到每个user所看过的所有的电影的组合
        Dataset<Row> joinedRatings = main.joinMovieRating(userRating);

        //过滤掉重复的组合
        Dataset<Row> uniqueRatings = main.filterDuplicate(joinedRatings);

        //做成<movie1:movie2> : <rating1，rating2>的pair
        Dataset<MoviePair> pairs = main.makePair(uniqueRatings);

        //group by (movie1,movie2),每一行都是所有的（rating1， rating2）的组合
        Dataset<Row> movieRatingPairs = main.groupByMoviePair(pairs);

        //计算两部电影之间的similarity，最后得到（movie：movie2）：（similarity， numPairs）
        Dataset<MovieSimilarity> movieSimilarities = main.computeSimilarity(movieRatingPairs);

        //把结果写出到文件里
        main.writeToLocalFile(movieSimilarities);

    }

    public Dataset<Row> getUserRating(SparkSession spark, String ratingPath) throws URISyntaxException {
        String path = ratingPath;
        Dataset<Row> df = spark
                .read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .csv(path)
                .toDF()
                .select("userId", "movieId", "rating");
        return df;
    }

    public Dataset<Row> joinMovieRating(Dataset<Row> userRating) {
        Dataset<Row> joinedRatings = userRating.as("a").join(
                userRating.as("b"),
                "userId")
                .selectExpr("a.userId as userId", "a.movieId as movie1",
                        "a.rating as rating1", "b.movieId as movie2", "b.rating as rating2");
        return joinedRatings;
    }

    public Dataset<Row> filterDuplicate(Dataset<Row> joinedRatings) {
        Dataset<Row> uniqueJoinedRatings = joinedRatings.filter(joinedRatings.col("movie1")
                .$less(joinedRatings.col("movie2")));
        return uniqueJoinedRatings;
    }

    public Dataset<MoviePair> makePair(Dataset<Row> uniqueRatings) {
        Dataset<MoviePair> pairs = uniqueRatings
                .map((MapFunction<Row, MoviePair>) row -> {
                    return new MoviePair(
                                    row.getAs("movie1"),
                                    row.getAs("movie2"),
                                    row.getAs("rating1"),
                                    row.getAs("rating2"));
                }, Encoders.bean(MoviePair.class));
        return pairs;
    }

    public Dataset<Row> groupByMoviePair(Dataset<MoviePair> uniqueRatings) {
        Dataset<Row> movieRatingPairs = uniqueRatings
                .groupBy("movie_pair")
                .agg(functions.collect_list("rating_pair").as("rating_pairs"));
        return movieRatingPairs;
    }

    public Dataset<MovieSimilarity> computeSimilarity(Dataset<Row> movieRatingPairs) {
        Dataset<MovieSimilarity> movieSimilarities = movieRatingPairs
                .map((MapFunction<Row, MovieSimilarity>) row-> {
                    WrappedArray<Integer> movie_pair = row.getAs("movie_pair");
                    WrappedArray<WrappedArray<Double>> rating_pairs = row.getAs("rating_pairs");
                    int num_pairs = rating_pairs.size();
                    double sum_xx = 0;
                    double sum_yy =0;
                    double sum_xy=0;
                    Iterator<WrappedArray<Double>> iterator = rating_pairs.iterator();
                    while (iterator.hasNext()) {
                        WrappedArray rating = iterator.next();
                        double rating1 = (double) rating.head();
                        double rating2 = (double) rating.last();
                        sum_xx += rating1*rating1;
                        sum_yy += rating2*rating2;
                        sum_xy += rating1*rating2;
                    }

                    double denominator = Math.sqrt(sum_xx) * Math.sqrt(sum_yy);
                    double score = 0;
                    if (denominator != 0) {
                        score = sum_xy/denominator;
                    }

            return new MovieSimilarity(movie_pair.head(), movie_pair.last(), score, num_pairs);
        }, Encoders.bean(MovieSimilarity.class));

        return movieSimilarities;
    }

    public void writeToLocalFile(Dataset<MovieSimilarity> movieSimilarities) {
        movieSimilarities.coalesce(10).write().csv(" ");
    }
}
