This first GTFS slice supports a narrow deterministic subset only.

Supported GTFS-like files:
- `agency.txt`
- `routes.txt`
- `trips.txt`
- `calendar.txt`
- `stop_times.txt`
- `stops.txt`
- `calendar_dates.txt`
- `fare_attributes.txt`
- `fare_rules.txt`

Supported derivation rules in this slice:
- one canonical trip row is produced per `trip_id + active service date`
- active service dates come from `calendar.txt` weekday/date ranges plus `calendar_dates.txt` additions and removals
- `origin` and `destination` come from the first and last `stop_times` rows using `stops.stop_code`
- `stops_count` is derived from the number of intermediate stops
- `departure_date` comes from the resolved active service dates after calendar expansion
- `carrier` comes from `agency.agency_name`
- `price_amount` and `currency` come from `fare_rules -> fare_attributes`
- `duration_minutes` is derived from first departure to last arrival time

Out of scope for this first slice:
- full GTFS calendar support
- frequencies-based service expansion
- frequencies
- shapes
- transfers
- live service updates
- complex fare rules
- advanced holiday/service merging beyond the narrow weekday + exception handling in this slice
