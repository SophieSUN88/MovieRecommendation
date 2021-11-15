import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;
//
import java.net.URISyntaxException;
//
public class MovieRecommendationTemplate {
//
    public static void main(String[] args) throws URISyntaxException {
        SparkSession spark = SparkSession.builder()
                .appName("MovieRecommendationApp")
                .master("local[*]")
                .getOrCreate();
        MovieRecommendationTemplate main = new MovieRecommendationTemplate();
    }

//        //从原始csv数据创建user, movie, rating的dataframe
//        Dataset<Row> userRating = main.getUserRating(spark);
//
//        //join userRating dataFrame来得到每个user所看过的所有的电影的组合
//        Dataset<Row> joinedRatings = main.joinMovieRating(userRating);
//
//        //过滤掉重复的组合
//        Dataset<Row> uniqueRatings = main.filterDuplicate(joinedRatings);
//
//        //做成<movie1:movie2> : <rating1，rating2>的pair
//        Dataset<MoviePair> pairs = main.makePair(uniqueRatings);
//
//        //group by (movie1,movie2),每一行都是所有的（rating1， rating2）的组合
//        Dataset<Row> movieRatingPairs = main.groupByMoviePair(pairs);
//
//        //计算两部电影之间的similarity，最后得到（movie1：movie2）：（similarity， number of Pairs）
//        Dataset<MovieSimilarity> movieSimilarities = main.computeSimilarity(movieRatingPairs);
//        movieSimilarities.show();
//
//        //把结果写出到文件里
//        main.writeToLocalFile(movieSimilarities);
//
//    }
}
