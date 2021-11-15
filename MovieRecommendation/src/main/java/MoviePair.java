import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class MoviePair {
    public List<Integer> movie_pair = new ArrayList<>();
    public List<Double> rating_pair = new ArrayList<>();

    public MoviePair(int movie1, int movie2, double rating1, double rating2) {
        movie_pair.add(movie1);
        movie_pair.add(movie2);
        rating_pair.add(rating1);
        rating_pair.add(rating2);
    }

    public List<Integer> getMovie_pair() {
        return movie_pair;
    }

    public void setMovie_pair(List<Integer> movie_pair) {
        this.movie_pair = movie_pair;
    }

    public List<Double> getRating_pair() {
        return rating_pair;
    }

    public void setRating_pair(List<Double> rating_pair) {
        this.rating_pair = rating_pair;
    }
}
