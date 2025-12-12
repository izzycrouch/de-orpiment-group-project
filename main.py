from extract_layer.utils.zip_layer_files import create_zip_layer,upload_zip_to_s3


res_l = create_zip_layer('layers')
res_p = create_zip_layer('pyarrow_layers')
# res_u = create_zip_layer('extract_layer')

upload_zip_to_s3(res_l,'libraries-layer-aci',"libraries.zip")
upload_zip_to_s3(res_p,'libraries-layer-aci',"pyarrow_libraries.zip")
# upload_zip_to_s3(res_u,"lambda-func-code-aci","extract_layer/utils.zip")

# from extract_layer.utils.save_data import save_data