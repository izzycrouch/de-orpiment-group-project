def create_dim_design(cleaned_design):
    dim_design = (
        cleaned_design[
            [
                'design_id',
                'design_name',
                'file_location',
                'file_name',
            ]
        ]
    )
    return dim_design