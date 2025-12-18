def create_dim_staff(cleaned_staff, cleaned_department):
    dim_staff = (
        cleaned_staff.merge(cleaned_department, on='department_id', how='inner')
        [
            [
                'staff_id', 
                'first_name', 
                'last_name',
                'department_name',
                'location',
                'email_address' 
            ]
        ]
    )
    return dim_staff
