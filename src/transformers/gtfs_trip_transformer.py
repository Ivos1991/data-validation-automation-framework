from __future__ import annotations

from datetime import timedelta

import pandas as pd

from src.framework.connectors.files.gtfs_loader import LoadedGtfsDataset
from src.framework.utils.date_utils import gtfs_time_to_minutes, parse_gtfs_service_date
from src.framework.utils.numeric_utils import normalize_float, normalize_int


GTFS_CANONICAL_TRIP_COLUMNS = [
    "trip_id",
    "origin",
    "destination",
    "departure_date",
    "stops_count",
    "route_id",
    "carrier",
    "price_amount",
    "currency",
    "duration_minutes",
]


class GtfsTripTransformer:
    """Transform the supported GTFS subset into canonical trip rows."""

    REQUIRED_COLUMNS = {
        "agency_frame": ["agency_id", "agency_name"],
        "routes_frame": ["route_id", "agency_id"],
        "trips_frame": ["route_id", "service_id", "trip_id"],
        "calendar_frame": ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"],
        "stop_times_frame": ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"],
        "stops_frame": ["stop_id", "stop_code"],
        "calendar_dates_frame": ["service_id", "date", "exception_type"],
        "fare_attributes_frame": ["fare_id", "price", "currency_type"],
        "fare_rules_frame": ["fare_id", "route_id"],
    }

    def transform(self, loaded_gtfs_dataset: LoadedGtfsDataset) -> pd.DataFrame:
        """Derive canonical trip rows from the supported GTFS input frames."""
        self._validate_required_columns(loaded_gtfs_dataset)
        trip_stops = self._build_trip_stop_frame(loaded_gtfs_dataset.stop_times_frame, loaded_gtfs_dataset.stops_frame)
        route_fares = self._build_route_fare_frame(
            loaded_gtfs_dataset.fare_rules_frame,
            loaded_gtfs_dataset.fare_attributes_frame,
        )
        service_dates = self._build_service_date_frame(
            loaded_gtfs_dataset.calendar_frame,
            loaded_gtfs_dataset.calendar_dates_frame,
        )
        if service_dates.empty:
            raise ValueError("GTFS calendar expansion did not produce any supported active service dates")

        trip_frame = loaded_gtfs_dataset.trips_frame.merge(
            loaded_gtfs_dataset.routes_frame[["route_id", "agency_id"]],
            on="route_id",
            how="left",
            validate="many_to_one",
        ).merge(
            loaded_gtfs_dataset.agency_frame[["agency_id", "agency_name"]],
            on="agency_id",
            how="left",
            validate="many_to_one",
        ).merge(
            trip_stops,
            on="trip_id",
            how="left",
            validate="one_to_one",
        ).merge(
            route_fares,
            on="route_id",
            how="left",
            validate="many_to_one",
        ).merge(
            service_dates[["service_id", "departure_date"]],
            on="service_id",
            how="inner",
            validate="many_to_many",
        )

        self._validate_transformed_trip_frame(trip_frame)

        trip_frame["trip_id"] = trip_frame["trip_id"].map(str) + "-" + trip_frame["departure_date"].str.replace("-", "", regex=False)
        trip_frame["origin"] = trip_frame["origin"].str.strip().str.upper()
        trip_frame["destination"] = trip_frame["destination"].str.strip().str.upper()
        trip_frame["route_id"] = trip_frame["route_id"].str.strip().str.upper()
        trip_frame["carrier"] = trip_frame["agency_name"].str.strip()
        trip_frame["price_amount"] = trip_frame["price"].map(normalize_float)
        trip_frame["currency"] = trip_frame["currency_type"].str.strip().str.upper()
        trip_frame["duration_minutes"] = trip_frame["duration_minutes"].map(normalize_int)
        trip_frame["stops_count"] = trip_frame["stops_count"].map(normalize_int)

        return trip_frame[
            [
                "trip_id",
                "origin",
                "destination",
                "departure_date",
                "stops_count",
                "route_id",
                "carrier",
                "price_amount",
                "currency",
                "duration_minutes",
            ]
        ].sort_values("trip_id").reset_index(drop=True)

    def _validate_required_columns(self, loaded_gtfs_dataset: LoadedGtfsDataset) -> None:
        for field_name, required_columns in self.REQUIRED_COLUMNS.items():
            frame = getattr(loaded_gtfs_dataset, field_name)
            missing_columns = [column for column in required_columns if column not in frame.columns]
            if missing_columns:
                raise ValueError(f"GTFS frame '{field_name}' is missing required columns: {', '.join(missing_columns)}")

    @staticmethod
    def _build_trip_stop_frame(stop_times_frame: pd.DataFrame, stops_frame: pd.DataFrame) -> pd.DataFrame:
        ordered_stop_times = stop_times_frame.copy()
        ordered_stop_times["stop_sequence"] = ordered_stop_times["stop_sequence"].astype(int)
        ordered_stop_times = ordered_stop_times.sort_values(["trip_id", "stop_sequence"]).reset_index(drop=True)

        first_stops = ordered_stop_times.groupby("trip_id", as_index=False).first()
        last_stops = ordered_stop_times.groupby("trip_id", as_index=False).last()
        stop_counts = ordered_stop_times.groupby("trip_id", as_index=False).size().rename(columns={"size": "stop_count"})

        stop_lookup = stops_frame[["stop_id", "stop_code"]].copy()
        first_stops = first_stops.merge(
            stop_lookup.rename(columns={"stop_id": "origin_stop_id", "stop_code": "origin"}),
            left_on="stop_id",
            right_on="origin_stop_id",
            how="left",
            validate="many_to_one",
        )
        last_stops = last_stops.merge(
            stop_lookup.rename(columns={"stop_id": "destination_stop_id", "stop_code": "destination"}),
            left_on="stop_id",
            right_on="destination_stop_id",
            how="left",
            validate="many_to_one",
        )

        trip_stop_frame = first_stops[["trip_id", "origin", "departure_time"]].merge(
            last_stops[["trip_id", "destination", "arrival_time"]],
            on="trip_id",
            how="inner",
            validate="one_to_one",
        ).merge(
            stop_counts,
            on="trip_id",
            how="inner",
            validate="one_to_one",
        )
        trip_stop_frame["duration_minutes"] = trip_stop_frame.apply(
            lambda row: gtfs_time_to_minutes(row["arrival_time"]) - gtfs_time_to_minutes(row["departure_time"]),
            axis=1,
        )
        trip_stop_frame["stops_count"] = trip_stop_frame["stop_count"].map(lambda stop_count: max(int(stop_count) - 2, 0))
        return trip_stop_frame[["trip_id", "origin", "destination", "duration_minutes", "stops_count"]]

    @staticmethod
    def _build_route_fare_frame(fare_rules_frame: pd.DataFrame, fare_attributes_frame: pd.DataFrame) -> pd.DataFrame:
        fare_frame = fare_rules_frame.merge(
            fare_attributes_frame[["fare_id", "price", "currency_type"]],
            on="fare_id",
            how="left",
            validate="many_to_one",
        )
        return fare_frame[["route_id", "price", "currency_type"]].drop_duplicates(subset=["route_id"]).reset_index(drop=True)

    @staticmethod
    def _validate_transformed_trip_frame(trip_frame: pd.DataFrame) -> None:
        required_fields = ["agency_name", "origin", "destination", "price", "currency_type", "duration_minutes", "stops_count"]
        for field_name in required_fields:
            if trip_frame[field_name].isna().any():
                raise ValueError(f"GTFS transformation could not derive required field '{field_name}' for all trips")

    @staticmethod
    def _build_service_date_frame(calendar_frame: pd.DataFrame, calendar_dates_frame: pd.DataFrame) -> pd.DataFrame:
        weekday_columns = {
            0: "monday",
            1: "tuesday",
            2: "wednesday",
            3: "thursday",
            4: "friday",
            5: "saturday",
            6: "sunday",
        }

        calendar_rows: list[dict[str, str]] = []
        for record in calendar_frame.to_dict(orient="records"):
            current_date = parse_gtfs_service_date(record["start_date"])
            end_date = parse_gtfs_service_date(record["end_date"])
            while current_date <= end_date:
                weekday_column = weekday_columns[current_date.weekday()]
                if int(record[weekday_column]) == 1:
                    calendar_rows.append(
                        {
                            "service_id": str(record["service_id"]).strip(),
                            "departure_date": current_date.isoformat(),
                        }
                    )
                current_date += timedelta(days=1)

        calendar_service_dates = pd.DataFrame(calendar_rows, columns=["service_id", "departure_date"])

        exception_dates = calendar_dates_frame.copy()
        exception_dates["departure_date"] = exception_dates["date"].map(lambda raw_value: parse_gtfs_service_date(raw_value).isoformat())

        added_dates = exception_dates[exception_dates["exception_type"].astype(int) == 1][["service_id", "departure_date"]]
        removed_dates = exception_dates[exception_dates["exception_type"].astype(int) == 2][["service_id", "departure_date"]]

        service_dates = pd.concat([calendar_service_dates, added_dates], ignore_index=True).drop_duplicates(
            subset=["service_id", "departure_date"]
        )
        if removed_dates.empty:
            return service_dates.reset_index(drop=True)

        removal_keys = set(zip(removed_dates["service_id"], removed_dates["departure_date"]))
        filtered_rows = [
            row for row in service_dates.to_dict(orient="records")
            if (row["service_id"], row["departure_date"]) not in removal_keys
        ]
        return pd.DataFrame(filtered_rows, columns=["service_id", "departure_date"]).reset_index(drop=True)
