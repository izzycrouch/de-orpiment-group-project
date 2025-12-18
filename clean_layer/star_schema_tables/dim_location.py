def create_dim_location(new_rows):
    return new_rows[
        [
            "address_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]
    ].rename(columns={"address_id": "location_id"})
