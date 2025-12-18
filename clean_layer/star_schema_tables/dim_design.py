from clean_layer.clean_func.clean_design import clean_design

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

#create_dim_design(clean_design('design/year=2025/month=12/day=15/batch_20251215T135936Z.parquet'))