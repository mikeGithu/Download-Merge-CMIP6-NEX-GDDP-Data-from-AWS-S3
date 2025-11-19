#!/usr/bin/env python
# coding: utf-8

# In[61]:


import os
import xarray as xr 
import s3fs
import netCDF4 as nc


# In[73]:


# Configuration
##bucket_path = "s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/MPI-ESM1-2-HR/historical/r1i1p1f1/pr" 
##s3://nex-gddp-cmip6/NEX-GDDP-CMIP6/<MPI-ESM1-2-HR>/<experiment>/<variable>/

bucket = "nex-gddp-cmip6"    
output_dir = "D:/PAU document/Thesis/Analysis/CMIP6 data/MPI-ESM1-2-HR/Historical/"
model = "MPI-ESM1-2-HR"
senario = "historical"
experiment = "r1i1p1f1"
variable = "pr"  # or tasmin, pr, etc.
start_year = 1983
end_year = 2014
os.makedirs(output_dir, exist_ok = True)


# In[74]:


# Connect to public S3 bucket
fs = s3fs.S3FileSystem(anon=True)
print(fs)


# In[75]:


# List all files in the directory
prefix = f"NEX-GDDP-CMIP6/{model}/{senario}/{variable}/"
##files = fs.ls(bucket_path + prefix)
#files = fs.ls(f"{bucket}/{prefix}")


# In[80]:


fs.ls(f"{bucket}/NEX-GDDP-CMIP6/{model}/")


# In[83]:


files = fs.ls(f"{bucket}/NEX-GDDP-CMIP6/{model}/historical/r1i1p1f1/pr/")


# In[84]:


print(files)


# In[85]:


# Filter by year range
filtered_files = [f for f in files if any(f"{year}.nc" in f for year in range(start_year, end_year + 1))]


# In[86]:


print(filtered_files)


# In[ ]:


# Download files locally
local_files = []
for s3_file in filtered_files:
    filename = os.path.basename(s3_file)
    local_path = os.path.join(output_dir, filename)
    if not os.path.exists(local_path):
        print(f"Downloading {filename}...")
        fs.get(s3_file, local_path)
    else:
        print(f"File {filename} already exists.")
    local_files.append(local_path)


# In[18]:


# Merge using xarray
print("Merging files...")
datasets = [xr.open_dataset(f) for f in local_files]
combined = xr.concat(datasets, dim="time")


# In[ ]:


# Save merged dataset
merged_path = os.path.join(output_dir, f"{model}_{experiment}_{variable}_{start_year}-{end_year}.nc")
combined.to_netcdf(merged_path)
print(f"Merged file saved to: {merged_path}")

