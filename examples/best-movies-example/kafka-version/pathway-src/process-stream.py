import time

import pathway as pw
from pathway.stdlib.utils.col import flatten_column

K = 3

rdkafka_settings = {
    "bootstrap.servers": "kafka:9092",
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
}


def compute_best(t_ratings, K):
    t_ratings = t_ratings.select(
        movieId=pw.apply_with_type(int, int, pw.this.movieId),
        rating=pw.apply_with_type(float, float, pw.this.rating),
    )
    t_best_ratings = t_ratings.groupby(pw.this.movieId).reduce(
        pw.this.movieId,
        sum_ratings=pw.reducers.sum(pw.this.rating),
        number_ratings=pw.reducers.count(pw.this.rating),
    )
    t_best_ratings = t_best_ratings.select(
        pw.this.movieId,
        pw.this.number_ratings,
        average_rating=pw.apply(
            lambda x, y: (x / y) if y != 0 else 0,
            pw.this.sum_ratings,
            pw.this.number_ratings,
        ),
    )
    t_best_ratings = t_best_ratings.select(
        movie_tuple=pw.apply(
            lambda x, y, z: (x, y, z),
            pw.this.average_rating,
            pw.this.number_ratings,
            pw.this.movieId,
        )
    )
    t_best_ratings = t_best_ratings.reduce(
        total_tuple=pw.reducers.sorted_tuple(pw.this.movie_tuple)
    )
    t_best_ratings = t_best_ratings.select(
        K_best=pw.apply(lambda my_tuple: (list(my_tuple))[-K:], pw.this.total_tuple)
    )
    t_best_ratings = flatten_column(t_best_ratings.K_best).select(pw.this.K_best)
    t_best_ratings = t_best_ratings.select(
        movieId=pw.apply(lambda rating_tuple: rating_tuple[2], pw.this.K_best),
        average_rating=pw.apply(lambda rating_tuple: rating_tuple[0], pw.this.K_best),
        views=pw.apply(lambda rating_tuple: rating_tuple[1], pw.this.K_best),
    )
    return t_best_ratings


t_ratings = pw.io.kafka.read(
    rdkafka_settings,
    topic_names=["ratings"],
    format="json",
    value_columns=[
        "movieId",
        "rating",
    ],
    types={"movieId": pw.Type.INT, "rating": pw.Type.FLOAT},
    autocommit_duration_ms=100,
)

t_best_ratings = compute_best(t_ratings, 3)

# We output the results in a dedicated CSV file
pw.io.csv.write(t_best_ratings, "./best_ratings.csv")

# We wait for Kafka to be ready.
time.sleep(20)

# We launch the computation.
pw.run()
