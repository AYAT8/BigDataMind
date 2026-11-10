# Gold data dictionary

This document is generated from the catalog defined in `spark/gold_catalog.py`.

## Shared rules

- Gold is always rebuilt from the valid Silver Delta table `data/silver/yellow_trips`.
- Day-of-week encoding matches Silver `pickup_dow`, where 1 = Sunday and 7 = Saturday.
- Revenue uses `SUM(total_amount)`; cash tips are not recorded in source totals.
- Rejected rows are excluded from all Gold aggregates.

## gold_hourly_demand

- Grain: one row per pickup_date and pickup_hour
- Purpose: Hourly trip volume and revenue demand by time of day.

### Columns

| Column | Type | Description |
|---|---|---|
| `pickup_date` | `date` | Calendar date for the pickup. |
| `pickup_hour` | `int` | Hour of the day in 24-hour format (0-23). |
| `trips` | `bigint` | Number of valid Silver trips in the slot. |
| `total_revenue` | `double` | Sum of total_amount for the slot. |
| `avg_fare` | `double` | Average fare_amount for trips in the slot. |
| `avg_distance_miles` | `double` | Average trip_distance in miles for trips in the slot. |
| `avg_duration_min` | `double` | Average trip_duration_min for trips in the slot. |

### Example questions (English)

- Which hour had the highest demand on 2024-01-15?
- What was the total revenue for pickup hour 18 in January 2024?
- How did average fare and trip length vary across morning and evening demand peaks?

### Example questions (Français)

- Quelle heure a connu la demande la plus élevée le 2024-01-15 ?
- Quel a été le revenu total pour l'heure de prise en charge 18 en janvier 2024 ?
- Comment le tarif moyen et la durée du trajet ont-ils varié entre les pics du matin et du soir ?

### Caveats

- Rejected rows are excluded because Gold is rebuilt from the valid Silver layer.
- Revenue and averages ignore nulls, and only valid Silver rows are counted.
- Cash tips are not recorded in total_amount per the source dictionary.

## gold_daily_demand

- Grain: one row per pickup_date
- Purpose: Daily demand and revenue, with a weekday label aligned to the Silver pickup_dow encoding.

### Columns

| Column | Type | Description |
|---|---|---|
| `pickup_date` | `date` | Calendar date for the pickup. |
| `day_of_week` | `int` | Spark day-of-week encoding from Silver pickup_dow: 1 = Sunday, 7 = Saturday. |
| `day_name` | `string` | Human-readable weekday name mapped from the Silver pickup_dow value. |
| `trips` | `bigint` | Total valid trips on the date. |
| `total_revenue` | `double` | Sum of total_amount for the date. |
| `avg_fare` | `double` | Average fare_amount for the date. |
| `avg_distance_miles` | `double` | Average trip_distance in miles for the date. |

### Example questions (English)

- Which day in January had the most trips?
- What was the weekday pattern for total revenue across the month?
- How many trips occurred on Sundays compared with Thursdays?

### Example questions (Français)

- Quel jour de janvier a enregistré le plus de courses ?
- Quel a été le profil hebdomadaire du revenu total au cours du mois ?
- Combien de courses ont eu lieu les dimanches par rapport aux jeudis ?

### Caveats

- Rejected rows are excluded before aggregation.
- day_of_week uses the same encoding as Silver pickup_dow and is not reinterpreted as Monday-first.
- Flex Fare rows can be kept with null passenger counts and other missing fields when they otherwise pass Silver validation.

## gold_revenue_by_zone

- Grain: one row per pickup zone
- Purpose: Trip counts and revenue grouped by pickup taxi zone and borough.

### Columns

| Column | Type | Description |
|---|---|---|
| `pu_location_id` | `int` | Pickup taxi-zone ID from Silver. |
| `zone` | `string` | Pickup zone name from the TLC lookup, including Unknown. |
| `borough` | `string` | Pickup borough name from the TLC lookup, including Unknown. |
| `trips` | `bigint` | Total valid trips for the pickup zone. |
| `total_revenue` | `double` | Sum of total_amount for the pickup zone. |
| `avg_fare` | `double` | Average fare_amount for the pickup zone. |

### Example questions (English)

- Which zone generated the most revenue in January?
- What is the average fare by borough?
- Which taxi zones still appear as Unknown in the pickup lookup?

### Example questions (Français)

- Quelle zone a généré le plus de revenus en janvier ?
- Quel est le tarif moyen par arrondissement ?
- Quelles zones de taxi apparaissent encore comme Inconnu dans la recherche de prise en charge ?

### Caveats

- Unmatched pickup zones stay in Gold as Unknown to preserve the full valid trip volume.
- This table aggregates by pickup zone only; drop-off zone is not used here.
- total_amount is the total charged to the passenger as reported by TLC; cash tips are not included.

## gold_peak_hours

- Grain: one row per day_of_week and pickup_hour
- Purpose: Busiest weekday-hour slots ranked by average trips per operating day.

### Columns

| Column | Type | Description |
|---|---|---|
| `day_of_week` | `int` | Spark day-of-week value in the same encoding as Silver pickup_dow. |
| `day_name` | `string` | Human-readable day label associated with the numeric weekday. |
| `pickup_hour` | `int` | Hour of day for the slot. |
| `trips` | `bigint` | Trips in that weekday-hour slot across the month. |
| `avg_trips_per_day` | `double` | Trips in the slot divided by the distinct pickup dates that fall on that weekday. |
| `avg_fare` | `double` | Average fare_amount for the weekday-hour slot. |
| `peak_rank` | `int` | Dense rank of weekday-hour slots by avg_trips_per_day descending, with 1 being busiest. |

### Example questions (English)

- What were the busiest weekday-hour slots by average daily demand?
- Which hour on Friday ranked highest after adjusting for the number of Fridays in the month?
- How did weekday evening demand compare with weekday midday demand?

### Example questions (Français)

- Quels étaient les créneaux les plus chargés par moyenne quotidienne de demande ?
- Quel créneau du vendredi a obtenu le meilleur classement après ajustement au nombre de vendredis du mois ?
- Comment la demande du soir en semaine se compare-t-elle à celle du milieu de journée ?

### Caveats

- peak_rank is a dense rank based on average trips per day, not raw trip counts.
- Slots are compared across all weekday-hour combinations; the denominator is the number of distinct pickup dates on that weekday in Silver.
- Unlike raw counts, this measure normalizes for uneven weekday frequency within the month.

## gold_payment_mix

- Grain: one row per payment type
- Purpose: Payment usage mix across valid Silver trips, including revenue share and total revenue.

### Columns

| Column | Type | Description |
|---|---|---|
| `payment_type_id` | `int` | Payment type code from Silver payment_type. |
| `payment_type_name` | `string` | Human-readable label from the TLC dictionary. |
| `trips` | `bigint` | Total valid trips for a payment type. |
| `total_revenue` | `double` | Sum of total_amount for the payment type. |
| `share_of_trips_pct` | `double` | Trips share of the total valid trip volume for the month, expressed as a percentage. |

### Example questions (English)

- What share of trips were paid by credit card in January?
- Which payment type had the highest total revenue?
- How does the payment mix compare between cash and Flex Fare?

### Example questions (Français)

- Quelle part des courses a été payée par carte bancaire en janvier ?
- Quel mode de paiement a généré le revenu total le plus élevé ?
- Comment le mix de paiement se compare-t-il entre cash et Flex Fare ?

### Caveats

- Rejected rows are excluded from the mix, so the percentages represent valid Silver trips only.
- Cash tips are not reported separately in the source; only reported total_amount is aggregated.
- Flex Fare rows remain eligible even when passenger_count or other fields are null, as long as all Silver validation rules pass.

## gold_distance_duration_stats

- Grain: one row per distance bucket
- Purpose: Distance-band summary for trip duration, fare and speed.

### Columns

| Column | Type | Description |
|---|---|---|
| `distance_bucket` | `string` | Bucket label for the trip-distance range. |
| `bucket_order` | `int` | Sorting order for the bucket labels, from 1 to 6. |
| `trips` | `bigint` | Trips in the distance bucket. |
| `avg_duration_min` | `double` | Average trip_duration_min for trips in the bucket. |
| `avg_fare` | `double` | Average fare_amount for trips in the bucket. |
| `avg_speed_mph` | `double` | Average speed in miles per hour computed per trip, averaged over the trips in the bucket; trips without a valid duration are excluded |

### Example questions (English)

- Which distance band has the longest average trip duration?
- How do average fare and speed change across the 0-1, 1-3, and 3-5 mile buckets?
- What is the trip volume in the 10-20 mile band?

### Example questions (Français)

- Quelle bande de distance a la durée moyenne de trajet la plus longue ?
- Comment le tarif moyen et la vitesse évoluent-ils entre les intervalles 0-1, 1-3 et 3-5 miles ?
- Quel est le volume de trajets dans la bande de distance 10-20 miles ?

### Caveats

- Buckets are defined on trip_distance with lower-bound inclusion and upper-bound exclusion: 0-1, 1-3, 3-5, 5-10, 10-20, and 20+.
- Exact boundaries such as 1.0, 3.0, 5.0, 10.0 and 20.0 are assigned to the intended bucket per the defined rules.
- Distance, duration, speed and shares are rounded only in the final projected Gold select.
