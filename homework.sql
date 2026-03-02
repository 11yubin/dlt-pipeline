-- Q1
SELECT min(trip_pickup_date_time), max(trip_pickup_date_time) FROM ny_taxi_data.taxi_trips;

-- Q2
SELECT 
      (COUNT(CASE WHEN payment_type = 'Credit' THEN 1 END) * 100.0 / COUNT(*)) AS proportion
  FROM ny_taxi_data.taxi_trips;

-- Q3
SELECT sum(tip_amt) FROM ny_taxi_data.taxi_trips;