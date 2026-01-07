def create_dim_location(cleaned_location):

    dim_location = (
        cleaned_location[
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
        ]
    )

    dim_location.rename(columns={"address_id": "location_id"}, inplace=True)

    return dim_location