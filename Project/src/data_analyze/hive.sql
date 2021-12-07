select region, avg(review_avg) avg_review, avg(rank) avg_rank, sum(count) sum_count from yogiyo group by region;

select region, avg(review_avg) avg_review, avg(rank) avg_rank, sum(count) sum_count from yogiyo group by region having avg_review > 4.73