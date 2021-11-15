public class MovieSimilarity {
    public int movie1;
    public int movie2;
    public double similarity;
    public int num_pairs;

    public MovieSimilarity(int movie1, int movie2, double similarity, int num_pairs) {
        this.movie1 = movie1;
        this.movie2 = movie2;
        this.similarity = similarity;
        this.num_pairs = num_pairs;
    }

    public int getMovie1() {
        return movie1;
    }

    public void setMovie1(int movie1) {
        this.movie1 = movie1;
    }

    public int getMovie2() {
        return movie2;
    }

    public void setMovie2(int movie2) {
        this.movie2 = movie2;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(double similarity) {
        this.similarity = similarity;
    }

    public int getNum_pairs() {
        return num_pairs;
    }

    public void setNum_pairs(int num_pairs) {
        this.num_pairs = num_pairs;
    }
}
